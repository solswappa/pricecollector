import aiohttp
import json
import logging
import asyncio
import websockets
import time
from config import settings
from .base import Exchange, catch_api_errors

logger = logging.getLogger(__name__)

class KrakenExchange(Exchange):
    def __init__(self):
        super().__init__()
        self.ws_url = "wss://ws.kraken.com"
        self.rest_url = "https://api.kraken.com/0/public"
        self.pairs = []
        # Kraken-specific settings
        self.MAX_CONNECTIONS = 2  # Conservative, since they prefer fewer connections
        self.MAX_PAIRS_PER_CONNECTION = 400  # Increased from 100, they support many per connection
        self.MAX_CONNECTION_TIME = None  # No specific limit mentioned
        self.PING_INTERVAL = 30  # Keep connection alive
        self.PONG_TIMEOUT = 60   # Conservative timeout
        self.HEARTBEAT_TIMEOUT = 5  # Should get heartbeat every 1s when subscribed
        self.channels = [
            "ticker",      # Most important - keep this
            # "trade",    # Comment out less critical channels
            # "spread",   # to reduce connection load
            # "book",
            # "ohlc"
        ]
        self.active_connections = 0
        self.MAX_MESSAGES_PER_SECOND = 20
        self.message_count = 0
        self.last_reset = time.time()
        self.last_heartbeat = None
        
    @catch_api_errors
    async def init_pairs(self):
        """Fetch all available pairs from Kraken and sort by volume"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.rest_url}/AssetPairs") as resp:
                data = await resp.json()
                if 'error' in data and data['error']:
                    raise Exception(f"Kraken API error: {data['error']}")
                logger.info(f"[Kraken] API Response Structure: {json.dumps(data)[:1000]}")
                
                # Sort pairs by volume
                sorted_pairs = sorted(data['result'].values(), key=lambda x: float(x.get('volume', 0)), reverse=True)
                
                # Get top pairs by volume
                self.pairs = [pair['wsname'] for pair in sorted_pairs if 'wsname' in pair][:self.MAX_PAIRS_PER_CONNECTION]
                
                logger.info(f"Selected top {len(self.pairs)} pairs by volume on Kraken")
        
    async def connect(self):
        await self.init_pairs()
        
        # Limit total number of pairs
        max_pairs = self.MAX_CONNECTIONS * self.MAX_PAIRS_PER_CONNECTION
        if len(self.pairs) > max_pairs:
            logger.warning(f"Limiting to {max_pairs} pairs to prevent connection overload")
            self.pairs = self.pairs[:max_pairs]
        
        # Create unique groups - ensure no duplicate pairs
        seen_pairs = set()
        connection_groups = []
        current_group = []
        
        for pair in self.pairs:
            if pair not in seen_pairs:
                seen_pairs.add(pair)
                current_group.append(pair)
                if len(current_group) >= self.MAX_PAIRS_PER_CONNECTION:
                    connection_groups.append(current_group)
                    current_group = []
        
        if current_group:  # Add any remaining pairs
            connection_groups.append(current_group)
        
        logger.info(f"[Kraken] Created {len(connection_groups)} unique connection groups")
        for i, group in enumerate(connection_groups):
            logger.debug(f"[Kraken] Group {i+1}: {len(group)} pairs")
        
        await asyncio.gather(*(
            self.handle_connection(pairs) 
            for pairs in connection_groups
        ))
        
    async def handle_connection(self, pairs):
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self.connection_start_time = time.time()
                    self.last_heartbeat = time.time()
                    
                    # Start health check and ping tasks
                    health_check = asyncio.create_task(self.connection_health_loop(ws))
                    ping_task = asyncio.create_task(self.ping_loop(ws))
                    
                    await self.subscribe(ws, pairs)
                    
                    try:
                        while True:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            
                            if isinstance(msg, bytes):
                                await ws.pong(msg)
                                continue
                                
                            data = json.loads(msg)
                            
                            # Handle heartbeat
                            if isinstance(data, dict) and data.get('event') == 'heartbeat':
                                self.last_heartbeat = time.time()
                                continue
                                
                            await self.process_message(data)
                            
                    finally:
                        health_check.cancel()
                        ping_task.cancel()
                        
            except Exception as e:
                await self.handle_api_error(
                    error_type="CONNECTION_FAILURE",
                    error_msg=str(e),
                    context={"pairs": pairs}
                )
                await self.handle_reconnect()
                
    async def check_connection_health(self, ws):
        """Kraken-specific health checks"""
        now = time.time()
        
        # Check for missing heartbeats
        if self.last_heartbeat and now - self.last_heartbeat > self.HEARTBEAT_TIMEOUT:
            logger.warning(f"[Kraken] No heartbeat received for {self.HEARTBEAT_TIMEOUT}s")
            await ws.close()
            return False
            
        # Check for stale connection
        if now - self.last_message_time > self.STALE_CONNECTION_TIMEOUT:
            logger.warning(f"[Kraken] Connection stale (no messages for {self.STALE_CONNECTION_TIMEOUT}s)")
            await ws.close()
            return False
            
        return True
    
    async def subscribe(self, ws, pairs):
        """Subscribe to ticker streams for given pairs"""
        try:
            subscribe_message = {
                "event": "subscribe",
                "pair": pairs,
                "subscription": {"name": "ticker"}
            }
            await ws.send(json.dumps(subscribe_message))
        except Exception as e:
            await self.handle_api_error(
                error_type="SUBSCRIPTION_FAILURE",
                error_msg=str(e),
                context={"pairs": pairs}
            )
        logger.info(f"Subscribed to ticker for {len(pairs)} pairs")
        await asyncio.sleep(1)  # Add delay between subscriptions
    
    async def process_message(self, msg):
        try:
            self.last_message_time = time.time()
            
            if isinstance(msg, list):
                channel_name = msg[2]
                pair = msg[3]
                data = msg[1]
                
                logger.debug(f"[Kraken] Received {channel_name} data for {pair}")

                if channel_name == "ticker":
                    price = float(data['c'][0])
                    volume = float(data['v'][1])
                    logger.info(f"[Kraken] {pair}: ${price:.2f} (vol: ${volume:.2f})")
                    await self.process_ticker(pair, data)
                elif channel_name == "trade":
                    await self.process_trades(pair, data)
                elif channel_name == "spread":
                    await self.process_spread(pair, data)
                elif channel_name == "book":
                    await self.process_book(pair, data)
                elif channel_name == "ohlc":
                    await self.process_ohlc(pair, data)
                    
        except Exception as e:
            await self.handle_api_error(
                error_type="MESSAGE_PROCESSING_ERROR",
                error_msg=str(e),
                context={"msg": str(msg)[:1000]}
            )
    
    async def process_ticker(self, pair, data):
        try:
            price = float(data['c'][0])
            volume_base = float(data['v'][1])
            volume_usd = volume_base * price  # Convert to USD
            
            await self.save_price(
                pair=pair,
                price=price,
                volume_24h=volume_usd,  # Store as USD
                vwap_24h=float(data['p'][1]),
                high_24h=float(data['h'][1]),
                low_24h=float(data['l'][1]),
                trades_24h=int(data['t'][1]),
                opening_price=float(data['o'][0])
            )
        except Exception as e:
            logger.error(f"[Kraken] Error processing ticker for {pair}: {e}", exc_info=True)
    
    async def heartbeat(self, ws):
        """Send periodic pings to keep connection alive"""
        ping_count = 0
        while True:
            try:
                await ws.ping()
                ping_count += 1
                logger.debug(f"Sent ping #{ping_count} to Kraken WebSocket")
                await asyncio.sleep(30)  # Kraken requires ping every 60s
            except Exception as e:
                logger.error(f"Kraken heartbeat error: {e}")
                break
    
    async def ping_loop(self, ws):
        """Kraken requires ping every 60s"""
        while True:
            try:
                await asyncio.sleep(30)
                await ws.ping()
            except Exception as e:
                await self.handle_api_error(
                    error_type="HEARTBEAT_FAILURE",
                    error_msg=str(e),
                    context={"last_heartbeat": self.last_heartbeat}
                )
                break