import json
import logging
import asyncio
import aiohttp
import websockets
import time
from .base import Exchange, catch_api_errors

logger = logging.getLogger(__name__)

class OKXExchange(Exchange):
    def __init__(self):
        super().__init__()
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
        self.rest_url = "https://www.okx.com/api/v5/public"
        self.pairs = []
        # OKX-specific settings
        self.MAX_CONNECTIONS = 3  # Match their connection request limit
        self.MAX_PAIRS_PER_CONNECTION = 300  # Conservative to ensure stability
        self.MAX_CONNECTION_TIME = None  # No specific limit
        self.PING_INTERVAL = 15  # Send ping every 15s (half of their 30s limit)
        self.PONG_TIMEOUT = 30   # They disconnect after 30s of no data
        self.STALE_CONNECTION_TIMEOUT = 30  # Match their timeout
        self.subscription_count = 0
        self.subscription_time = time.time()
        self.MAX_SUBSCRIPTIONS_PER_HOUR = 480  # Their explicit limit
        
    @catch_api_errors
    async def init_pairs(self):
        """Fetch available pairs from OKX and sort by volume"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.rest_url}/instruments?instType=SPOT") as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    raise Exception(f"Failed to fetch OKX pairs: {error_text}")
                
                data = await resp.json()
                
                # Sort pairs by volume
                sorted_pairs = sorted(data.get('data', []), key=lambda x: float(x.get('volCcy24h', 0)), reverse=True)
                
                # Get top pairs by volume
                self.pairs = [inst['instId'] for inst in sorted_pairs if inst.get('state') == 'live'][:self.MAX_PAIRS_PER_CONNECTION]
                
                logger.info(f"Selected top {len(self.pairs)} pairs by volume on OKX")
        
    async def connect(self):
        await self.init_pairs()
        
        # Split pairs into groups
        chunk_size = self.MAX_PAIRS_PER_CONNECTION
        pair_groups = [
            self.pairs[i:i + chunk_size]
            for i in range(0, len(self.pairs), chunk_size)
        ]
        
        await asyncio.gather(*(
            self.handle_connection(group) 
            for group in pair_groups
        ))
        
    async def handle_connection(self, pairs):
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    await self.subscribe(ws, pairs)
                    
                    while True:
                        try:
                            msg = await ws.recv()
                            await self.process_message(json.loads(msg))
                        except Exception as e:
                            logger.error(f"[OKX] Message error: {e}")
                            break
                            
            except Exception as e:
                await self.handle_api_error(
                    error_type="CONNECTION_FAILURE",
                    error_msg=str(e),
                    context={"pairs": pairs}
                )
                await asyncio.sleep(5)
    
    async def subscribe(self, ws, pairs):
        """Subscribe with rate limiting"""
        now = time.time()
        # Reset subscription count if an hour has passed
        if now - self.subscription_time > 3600:
            self.subscription_count = 0
            self.subscription_time = now
            
        if self.subscription_count >= self.MAX_SUBSCRIPTIONS_PER_HOUR:
            logger.warning("[OKX] Subscription limit reached, waiting for reset")
            await asyncio.sleep(3600 - (now - self.subscription_time))
            self.subscription_count = 0
            self.subscription_time = time.time()
            
        try:
            subscribe_msg = {
                "op": "subscribe",
                "args": [{
                    "channel": "tickers",
                    "instId": pair
                } for pair in pairs]
            }
            await ws.send(json.dumps(subscribe_msg))
        except Exception as e:
            await self.handle_api_error(
                error_type="SUBSCRIPTION_FAILURE",
                error_msg=str(e),
                context={"pairs": pairs}
            )
        self.subscription_count += 1
        logger.info(f"[OKX] Subscribed to {len(pairs)} pairs")
    
    async def process_message(self, msg):
        try:
            self.last_message_time = time.time()
            
            if 'data' in msg:
                for ticker in msg['data']:
                    pair = ticker['instId']
                    price = float(ticker['last'])
                    volume_base = float(ticker['vol24h'])
                    volume_usd = volume_base * price  # Convert to USD
                    
                    await self.save_price(
                        pair=pair,
                        price=price,
                        volume_24h=volume_usd,  # Store as USD
                        vwap_24h=float(ticker.get('sodUtc0', 0)),
                        high_24h=float(ticker['high24h']),
                        low_24h=float(ticker['low24h']),
                        trades_24h=int(float(ticker.get('trades24h', 0))),
                        opening_price=float(ticker.get('open24h', 0))
                    )
        except Exception as e:
            await self.handle_api_error(
                error_type="MESSAGE_PROCESSING_ERROR",
                error_msg=str(e),
                context={"msg": str(msg)[:1000]}
            )
    
    async def check_connection_health(self, ws):
        """OKX-specific health checks"""
        now = time.time()
        
        # Check for stale connection (30s limit)
        if now - self.last_message_time > self.STALE_CONNECTION_TIMEOUT:
            logger.warning(f"[OKX] Connection stale (no messages for {self.STALE_CONNECTION_TIMEOUT}s)")
            await ws.close()
            return False
            
        return True

    async def ping_loop(self, ws):
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