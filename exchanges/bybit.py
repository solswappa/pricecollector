import json
import logging
import asyncio
import aiohttp
import websockets
import time
from .base import Exchange, catch_api_errors

logger = logging.getLogger(__name__)

class BybitExchange(Exchange):
    def __init__(self):
        super().__init__()
        self.ws_url = "wss://stream.bybit.com/v5/public/spot"
        self.rest_url = "https://api.bybit.com/v5/market"
        self.pairs = []
        # Bybit-specific settings
        self.MAX_CONNECTION_TIME = 600  # 10 minutes default
        self.PING_INTERVAL = 20   # They recommend ping every 20s
        self.PONG_TIMEOUT = 30    # Conservative timeout for pong response
        self.STALE_CONNECTION_TIMEOUT = 60  # Conservative timeout
        self.MAX_CONNECTIONS = 5  # Increased from 1 to 5
        self.MAX_PAIRS_PER_CONNECTION = 200  # Increased to 200 pairs per connection
        
    @catch_api_errors
    async def init_pairs(self):
        """Fetch available pairs from Bybit and sort by volume"""
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        }
        
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(f"{self.rest_url}/tickers?category=spot") as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    raise Exception(f"API error ({resp.status}): {error_text}")
                
                data = await resp.json()
                logger.info(f"[Bybit] API Response Structure: {json.dumps(data)[:1000]}")
                
                # Sort pairs by volume
                sorted_pairs = sorted(data.get('result', {}).get('list', []), key=lambda x: float(x['volume24h']), reverse=True)
                
                # Get top pairs by volume
                self.pairs = [ticker['symbol'] for ticker in sorted_pairs][:self.MAX_PAIRS_PER_CONNECTION]
                
                logger.info(f"[Bybit] Selected top {len(self.pairs)} pairs by volume")
    
    async def heartbeat(self, ws):
        """Send periodic pings to keep connection alive"""
        while True:
            try:
                ping_msg = {
                    "op": "ping"
                }
                logger.debug("[Bybit] Sending ping")
                await ws.send(json.dumps(ping_msg))
                await asyncio.sleep(20)
            except Exception as e:
                await self.handle_api_error(
                    error_type="HEARTBEAT_FAILURE",
                    error_msg=str(e),
                    context={"last_heartbeat": self.last_heartbeat}
                )
                break
    
    async def handle_connection(self, pairs):
        while True:
            try:
                logger.info(f"[Bybit] Connecting to WebSocket for {len(pairs)} pairs...")
                async with websockets.connect(self.ws_url) as ws:
                    # Start heartbeat
                    heartbeat_task = asyncio.create_task(self.heartbeat(ws))
                    
                    await self.subscribe(ws, pairs)
                    
                    while True:
                        try:
                            msg = await ws.recv()
                            logger.debug(f"[Bybit] Raw message received: {msg}")  # Debug raw message
                            
                            data = json.loads(msg)
                            if data.get('ret_msg') == 'pong':
                                logger.debug("[Bybit] Received pong")
                                continue
                                
                            if data.get('success') is True:
                                logger.debug(f"[Bybit] Subscription confirmed: {data}")
                                continue
                                
                            if 'topic' not in data:
                                logger.warning(f"[Bybit] Unexpected message format: {data}")
                                continue
                                
                            await self.process_message(data)
                            
                        except websockets.exceptions.ConnectionClosed as e:
                            logger.error(f"[Bybit] WebSocket closed: {e}")
                            break
                        except json.JSONDecodeError as e:
                            logger.error(f"[Bybit] JSON decode error: {e}, raw message: {msg}")
                            continue
                        except Exception as e:
                            await self.handle_api_error(
                                error_type="MESSAGE_PROCESSING_ERROR",
                                error_msg=str(e),
                                context={"msg": str(msg)[:1000]}  # Truncate long messages
                            )
                            continue
                    
                    heartbeat_task.cancel()
                    
            except Exception as e:
                await self.handle_api_error(
                    error_type="CONNECTION_FAILURE",
                    error_msg=str(e),
                    context={"pairs": pairs}
                )
                await asyncio.sleep(5)
    
    async def subscribe(self, ws, pairs):
        """Subscribe to ticker streams in batches of 10 (Bybit's recommended batch size)"""
        try:
            batch_size = 10  # Bybit's limit per subscription request
            for i in range(0, len(pairs), batch_size):
                batch = pairs[i:i + batch_size]
                logger.info(f"[Bybit] Subscribing to batch: {batch}")
                subscribe_msg = {
                    "op": "subscribe",
                    "args": [f"tickers.{pair}" for pair in batch]
                }
                await ws.send(json.dumps(subscribe_msg))
                await asyncio.sleep(0.5)  # Small delay between batches to avoid overwhelming
        except Exception as e:
            await self.handle_api_error(
                error_type="SUBSCRIPTION_FAILURE",
                error_msg=str(e),
                context={"pairs": pairs}
            )
    
    async def process_message(self, msg):
        try:
            self.last_message_time = time.time()
            
            if msg.get('topic', '').startswith('tickers.'):
                data = msg.get('data', {})
                if not data:
                    return
                    
                pair = data['symbol']
                price = float(data['lastPrice'])
                volume_base = float(data['volume24h'])
                volume_usd = volume_base * price  # Convert to USD
                
                await self.save_price(
                    pair=pair,
                    price=price,
                    volume_24h=volume_usd,  # Store as USD
                    vwap_24h=float(data.get('price24hPcnt', 0)),
                    high_24h=float(data['highPrice24h']),
                    low_24h=float(data['lowPrice24h']),
                    trades_24h=int(float(data.get('turnover24h', 0))),
                    opening_price=float(data.get('prevPrice24h', 0))
                )
        except Exception as e:
            await self.handle_api_error(
                error_type="MESSAGE_PROCESSING_ERROR",
                error_msg=str(e),
                context={"msg": str(msg)[:1000]}  # Truncate long messages
            )
    
    async def connect(self):
        """Implement required abstract method"""
        await self.init_pairs()
        
        # Split pairs into groups
        chunk_size = self.MAX_PAIRS_PER_CONNECTION
        pair_groups = [
            self.pairs[i:i + chunk_size]
            for i in range(0, len(self.pairs), chunk_size)
        ]
        
        logger.info(f"[Bybit] Starting {len(pair_groups)} connections")
        
        # Start connections for each group
        await asyncio.gather(*(
            self.handle_connection(group) 
            for group in pair_groups
        )) 
    
    async def check_connection_health(self, ws):
        """Bybit-specific health checks"""
        now = time.time()
        
        # Check max connection time (10 min default)
        if now - self.connection_start_time > self.MAX_CONNECTION_TIME:
            logger.warning(f"[Bybit] Connection exceeded max time of {self.MAX_CONNECTION_TIME}s")
            await ws.close()
            return False
            
        # Check for stale connection
        if now - self.last_message_time > self.STALE_CONNECTION_TIMEOUT:
            logger.warning(f"[Bybit] Connection stale (no messages for {self.STALE_CONNECTION_TIMEOUT}s)")
            await ws.close()
            return False
            
        return True