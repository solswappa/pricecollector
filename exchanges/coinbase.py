import json
import logging
import asyncio
import websockets
import aiohttp
import time
from .base import Exchange, catch_api_errors

logger = logging.getLogger(__name__)

class CoinbaseExchange(Exchange):
    def __init__(self):
        super().__init__()
        self.ws_url = "wss://ws-feed.exchange.coinbase.com"
        self.rest_url = "https://api.exchange.coinbase.com"
        self.pairs = []
        self.MAX_PAIRS_PER_CONNECTION = 400
        self.MAX_CONNECTIONS = 2
        # Coinbase-specific settings
        self.MAX_CONNECTION_TIME = None  # No specific limit
        self.PING_INTERVAL = 30  # Send ping every 30s
        self.PONG_TIMEOUT = 60   # Conservative timeout
        self.STALE_CONNECTION_TIMEOUT = 10  # Heartbeat should come every 1s
        self.last_heartbeat = None
        
    @catch_api_errors
    async def init_pairs(self):
        """Fetch available pairs from Coinbase and sort by volume"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.rest_url}/products/stats") as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    raise Exception(f"Failed to fetch Coinbase pairs: {error_text}")
                
                stats_data = await resp.json()
                async with session.get(f"{self.rest_url}/products") as resp:
                    products_data = await resp.json()
                    
                    # Combine product info with volume data
                    pairs_with_volume = []
                    for product in products_data:
                        product_id = product['id']
                        volume = float(stats_data.get(product_id, {}).get('volume', 0))
                        if product['status'] == 'online' and not product['trading_disabled']:
                            pairs_with_volume.append({
                                'id': product_id,
                                'volume': volume
                            })
                    
                    # Sort pairs by volume
                    sorted_pairs = sorted(pairs_with_volume, key=lambda x: x['volume'], reverse=True)
                    
                    # Get top pairs by volume
                    self.pairs = [p['id'] for p in sorted_pairs][:self.MAX_PAIRS_PER_CONNECTION]
                    
                    logger.info(f"Selected top {len(self.pairs)} pairs by volume on Coinbase")
        
    async def subscribe(self, ws, pairs):
        """Implement required abstract method"""
        try:
            subscribe_msg = {
                "type": "subscribe",
                "product_ids": pairs,
                "channels": ["ticker"]
            }
            await ws.send(json.dumps(subscribe_msg))
            logger.info(f"[Coinbase] Subscribed to {len(pairs)} pairs")
        except Exception as e:
            await self.handle_api_error(
                error_type="SUBSCRIPTION_FAILURE",
                error_msg=str(e),
                context={"pairs": pairs}
            )
        
    async def connect(self):
        await self.init_pairs()
        while True:
            try:
                self.connection_start_time = time.time()
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=self.PING_INTERVAL,
                    ping_timeout=self.PONG_TIMEOUT,
                    close_timeout=10
                ) as ws:
                    ping_task = asyncio.create_task(self.ping_loop(ws))
                    health_task = asyncio.create_task(self.health_check_loop(ws))
                    
                    try:
                        await self.subscribe(ws, self.pairs)
                        while True:
                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=30)
                                await self.process_message(json.loads(msg))
                            except asyncio.TimeoutError:
                                logger.warning("[Coinbase] Message timeout")
                                if not ws.open:
                                    raise websockets.exceptions.ConnectionClosed(
                                        rcvd=None, sent=None
                                    )
                    finally:
                        ping_task.cancel()
                        health_task.cancel()
                        
            except Exception as e:
                await self.handle_api_error(
                    error_type="CONNECTION_FAILURE",
                    error_msg=str(e),
                    context={"reconnect_attempt": True}
                )
                await self.handle_reconnect()

    async def ping_loop(self, ws):
        """Coinbase requires ping every 30s"""
        while True:
            try:
                await asyncio.sleep(15)
                await ws.ping()
            except Exception as e:
                await self.handle_api_error(
                    error_type="HEARTBEAT_FAILURE",
                    error_msg=str(e),
                    context={"last_heartbeat": self.last_heartbeat}
                )
                break

    async def process_message(self, msg):
        try:
            self.last_message_time = time.time()
            
            if msg.get('type') == 'ticker':
                pair = msg['product_id']
                price = float(msg['price'])
                volume_base = float(msg['volume_24h'])
                volume_usd = volume_base * price  # Convert to USD
                
                await self.save_price(
                    pair=pair,
                    price=price,
                    volume_24h=volume_usd,  # Store as USD
                    vwap_24h=float(msg.get('vwap', 0)),
                    high_24h=float(msg['high_24h']),
                    low_24h=float(msg['low_24h']),
                    trades_24h=int(float(msg.get('trades_24h', 0))),
                    opening_price=float(msg.get('open_24h', 0))
                )
        except Exception as e:
            await self.handle_api_error(
                error_type="MESSAGE_PROCESSING_ERROR",
                error_msg=str(e),
                context={"msg": str(msg)[:1000]}
            )

    async def check_connection_health(self, ws):
        """Coinbase-specific health checks"""
        now = time.time()
        
        # Check for missing heartbeats (should get one every second)
        if self.last_heartbeat and now - self.last_heartbeat > self.STALE_CONNECTION_TIMEOUT:
            logger.warning(f"[Coinbase] No heartbeat received for {self.STALE_CONNECTION_TIMEOUT}s")
            await ws.close()
            return False
            
        # Check for stale connection
        if now - self.last_message_time > self.STALE_CONNECTION_TIMEOUT:
            logger.warning(f"[Coinbase] Connection stale (no messages for {self.STALE_CONNECTION_TIMEOUT}s)")
            await ws.close()
            return False
            
        return True 

    async def health_check_loop(self, ws):
        """Continuous health monitoring"""
        while True:
            try:
                await asyncio.sleep(5)
                now = time.time()
                
                if now - self.last_message_time > self.STALE_CONNECTION_TIMEOUT:
                    error_context = {
                        'last_message': self.last_message_time,
                        'timeout_threshold': self.STALE_CONNECTION_TIMEOUT,
                        'time_since_last': now - self.last_message_time
                    }
                    
                    async with self.pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO exchange_errors 
                            (exchange, error_type, error_message, context)
                            VALUES ($1, $2, $3, $4)
                        """,
                        self.name,
                        'STALE_CONNECTION',
                        'No messages received within timeout period',
                        json.dumps(error_context))
                        
                    await ws.close()
                    break
                    
            except Exception as e:
                logger.error(f"[Coinbase] Health check error: {e}")
                break 