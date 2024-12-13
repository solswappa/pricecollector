import json
import logging
import asyncio
import aiohttp
import websockets
import time
from .base import Exchange, catch_api_errors

logger = logging.getLogger(__name__)

class BinanceExchange(Exchange):
    def __init__(self):
        super().__init__()
        self.ws_url = "wss://stream.binance.com:9443/ws"
        self.rest_url = "https://api.binance.com/api/v3"
        self.pairs = []
        self.MAX_STREAMS_PER_CONN = 1024
        self.MAX_MESSAGES_PER_SECOND = 5
        self.MAX_CONNECTION_TIME = 24 * 60 * 60  # 24 hours
        self.PING_INTERVAL = 180  # 3 minutes
        self.PONG_TIMEOUT = 600   # 10 minutes
        self.MAX_CONNECTIONS = 3
        self.MAX_PAIRS_PER_CONNECTION = 200
        
    @catch_api_errors
    async def init_pairs(self):
        """Fetch all trading pairs from Binance and sort by volume"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.rest_url}/ticker/24hr") as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    raise Exception(f"Failed to fetch Binance pairs: {error_text}")
                
                data = await resp.json()
                logger.info(f"[Binance] API Response Structure: {json.dumps(data)[:1000]}")
                
                # Sort pairs by volume
                sorted_pairs = sorted(data, key=lambda x: float(x['quoteVolume']), reverse=True)
                
                # Get top pairs by volume
                self.pairs = [item['symbol'] for item in sorted_pairs][:self.MAX_PAIRS_PER_CONNECTION]
                
                logger.info(f"Selected top {len(self.pairs)} pairs by volume on Binance")

    async def subscribe(self, ws):
        """Subscribe to ticker streams for all pairs"""
        try:
            streams = [f"{pair.lower()}@ticker" for pair in self.pairs]
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": streams,
                "id": 1
            }
            await ws.send(json.dumps(subscribe_msg))
            logger.info(f"[Binance] Subscribed to {len(streams)} streams")
        except Exception as e:
            await self.handle_api_error(
                error_type="SUBSCRIPTION_FAILURE",
                error_msg=str(e),
                context={"streams": streams}
            )
        
    async def connect(self):
        await self.init_pairs()
        
        while True:
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=self.PING_INTERVAL,
                    ping_timeout=self.PONG_TIMEOUT,
                    close_timeout=10
                ) as ws:
                    await self.handle_websocket(ws, self.handle_connection)
                    
            except Exception as e:
                logger.error(f"[Binance] Connection error: {e}")
                await self.handle_reconnect()
                
    async def handle_connection(self, ws):
        """Handle the main WebSocket connection loop"""
        await self.subscribe(ws)
        
        while True:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=30)
                data = json.loads(msg)
                
                if "ping" in data:
                    await ws.pong(json.dumps({"pong": data["ping"]}))
                    continue
                    
                if "e" in data and data["e"] == "24hrTicker":
                    await self.process_message(data)
                    
            except asyncio.TimeoutError:
                logger.warning("[Binance] WebSocket timeout, sending ping")
                await ws.ping()
            except websockets.exceptions.ConnectionClosed:
                logger.warning("[Binance] WebSocket closed")
                break
            except Exception as e:
                await self.handle_api_error(
                    error_type="CONNECTION_FAILURE",
                    error_msg=str(e),
                    context={"reconnect_attempt": True}
                )

    async def process_message(self, data):
        """Process ticker data"""
        try:
            self.last_message_time = time.time()
            
            if data["e"] == "24hrTicker":
                price = float(data["c"])
                volume_base = float(data["v"])
                volume_usd = volume_base * price  # Convert to USD
                
                await self.save_price(
                    pair=data["s"],
                    price=price,
                    volume_24h=volume_usd,  # Store as USD
                    vwap_24h=float(data["w"]),
                    high_24h=float(data["h"]),
                    low_24h=float(data["l"]),
                    trades_24h=int(data["n"]),
                    opening_price=float(data["o"])
                )
        except Exception as e:
            logger.error(f"[Binance] Process error: {e}")