import asyncio
import logging
import signal
from config import settings
from exchanges.kraken import KrakenExchange
from exchanges.binance import BinanceExchange
from exchanges.coinbase import CoinbaseExchange
from exchanges.okx import OKXExchange
from exchanges.bybit import BybitExchange
import json
import traceback
import time

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

EXCHANGE_CLASSES = {
    "KrakenExchange": KrakenExchange,
    "BinanceExchange": BinanceExchange,
    "CoinbaseExchange": CoinbaseExchange,
    "OKXExchange": OKXExchange,
    "BybitExchange": BybitExchange,
}

class ExchangeManager:
    def __init__(self):
        self.exchanges = []
        self.tasks = []
        self.shutdown_event = asyncio.Event()
        
    async def init_exchanges(self):
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.exchanges = [
                    EXCHANGE_CLASSES[name]() 
                    for name in settings.ENABLED_EXCHANGES
                ]
                
                for exchange in self.exchanges:
                    await exchange.init_pool()
                    await asyncio.sleep(1)
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Database connection attempt {attempt + 1} failed, retrying in {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                else:
                    raise
        
    async def monitor_exchanges(self):
        """Monitor exchange health and log aggregate status"""
        while not self.shutdown_event.is_set():
            # Wait for at least one exchange to be initialized
            if not self.exchanges or not any(e.pool for e in self.exchanges):
                await asyncio.sleep(5)
                continue

            status = {
                'active_exchanges': len([e for e in self.exchanges if e.pool]),
                'total_errors': sum(e.error_count for e in self.exchanges),
                'connection_ages': {
                    e.name: time.time() - e.connection_start_time 
                    for e in self.exchanges
                }
            }
            
            try:
                # Find first available pool
                pool = next((e.pool for e in self.exchanges if e.pool), None)
                if pool:
                    async with pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO exchange_status 
                            (exchange, status, metadata)
                            VALUES ($1, $2, $3)
                        """,
                        "ExchangeManager",
                        "RUNNING" if status['active_exchanges'] > 0 else "DEGRADED",
                        json.dumps(status)
                        )
            except Exception as e:
                logger.error(f"Failed to log manager status: {e}")
            
            await asyncio.sleep(30)

    async def start(self):
        self.monitor_task = asyncio.create_task(self.monitor_exchanges())
        while not self.shutdown_event.is_set():
            try:
                await self.init_exchanges()
                self.tasks = [
                    asyncio.create_task(exchange.connect())
                    for exchange in self.exchanges
                ]
                await asyncio.gather(*self.tasks)
            except Exception as e:
                try:
                    async with self.exchanges[0].pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO exchange_errors 
                            (exchange, error_type, error_message, context)
                            VALUES ($1, $2, $3, $4)
                        """, 
                        "ExchangeManager",
                        "CRITICAL_ERROR",
                        str(e),
                        json.dumps({"traceback": traceback.format_exc()})
                        )
                except Exception:
                    pass

                for task in self.tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*self.tasks, return_exceptions=True)
                await asyncio.sleep(5)
        
    async def shutdown(self):
        self.shutdown_event.set()
        for exchange in self.exchanges:
            if exchange.pool:
                await exchange.pool.close()

async def main():
    manager = ExchangeManager()
    
    def signal_handler():
        logger.info("Received shutdown signal")
        asyncio.create_task(manager.shutdown())
        
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_event_loop().add_signal_handler(sig, signal_handler)
        
    try:
        await manager.start()
    finally:
        await manager.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
