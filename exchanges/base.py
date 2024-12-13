import asyncpg
from abc import ABC, abstractmethod
from datetime import datetime
from config import settings
import logging
import time
import asyncio
import json
import psutil
import socket
import traceback
import sys
import io
import aiohttp

logger = logging.getLogger(__name__)

def catch_api_errors(func):
    async def wrapper(self, *args, **kwargs):
        try:
            return await func(self, *args, **kwargs)
        except aiohttp.ClientError as e:
            await self.handle_api_error(
                error_type="API_CONNECTION_ERROR",
                error_msg=str(e),
                raw_error=e,
                context={
                    "method": func.__name__,
                    "exchange": self.name,
                    "url": getattr(self, 'rest_url', None)
                }
            )
        except Exception as e:
            await self.handle_api_error(
                error_type="INITIALIZATION_ERROR",
                error_msg=str(e),
                raw_error=e
            )
    return wrapper

class Exchange(ABC):
    _pool = None  # Class-level shared pool
    _pool_lock = asyncio.Lock()

    @classmethod
    async def get_pool(cls):
        async with cls._pool_lock:
            if cls._pool is None:
                cls._pool = await asyncpg.create_pool(
                    database=settings.DB_NAME,
                    host=settings.DB_HOST,
                    user=settings.DB_USER,
                    password=settings.DB_PASS,
                    min_size=2,
                    max_size=10,
                    command_timeout=10
                )
            return cls._pool

    def __init__(self):
        self.pool = None  # Will be set to class pool
        self._pool_lock = asyncio.Lock()
        self.last_update = None
        self.name = self.__class__.__name__
        self.ws_reconnect_delay = settings.WS_RECONNECT_DELAY
        self.ws_ping_interval = settings.WS_PING_INTERVAL
        self.ws_timeout = settings.WS_TIMEOUT
        self.max_pairs = settings.MAX_PAIRS_PER_EXCHANGE
        self.last_updates = {}  # Track last update time per pair
        self.update_thresholds = {
            'high': 15,    # 15 seconds for major pairs
            'medium': 30,  # 30 seconds for medium activity
            'low': 60      # 1 minute for low activity pairs
        }
        self.reconnect_delay = 5  # Base delay in seconds
        self.max_reconnect_delay = 300  # Max 5 minutes
        self.current_reconnect_delay = self.reconnect_delay
        self.error_count = 0
        self.last_error = None
        self.start_time = datetime.utcnow()
        self.last_message_time = time.time()
        self.connection_start_time = time.time()
        
        # Connection health settings
        self.MAX_CONNECTION_TIME = 24 * 60 * 60  # 24 hours
        self.PING_INTERVAL = 180  # 3 minutes
        self.PONG_TIMEOUT = 600   # 10 minutes
        self.STALE_CONNECTION_TIMEOUT = 600  # 10 minutes
        
        # Add these base attributes
        self.MAX_CONNECTIONS = 1
        self.MAX_PAIRS_PER_CONNECTION = 100
        
    async def init_pool(self):
        self.pool = await self.get_pool()
    
    async def handle_websocket(self, ws, handler):
        """Generic WebSocket handler with DB-only error logging"""
        while True:
            try:
                await self.log_connection_event('CONNECT')
                health_check = asyncio.create_task(self.connection_health_loop(ws))
                
                try:
                    await handler(ws)
                finally:
                    health_check.cancel()
                    
            except Exception as e:
                await self.log_connection_event('FAIL', str(e))
                await self.handle_reconnect()
                
    async def handle_reconnect(self):
        """Enhanced reconnect handling with logging"""
        error_context = {
            'last_message_time': self.last_message_time,
            'connection_duration': time.time() - self.connection_start_time,
            'error_count': self.error_count
        }
        
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO exchange_errors 
                    (exchange, error_type, error_message, context)
                    VALUES ($1, $2, $3, $4)
                """, 
                self.name, 
                'CONNECTION_LOST',
                'Connection terminated',
                json.dumps(error_context)
                )
        except Exception:
            pass  # Silently fail if DB logging fails

        await asyncio.sleep(self.current_reconnect_delay)
        self.current_reconnect_delay = min(
            self.current_reconnect_delay * 2,
            self.max_reconnect_delay
        )
    
    async def safe_ws_send(self, ws, message):
        """Send WebSocket message with error handling"""
        try:
            await ws.send(json.dumps(message))
            return True
        except Exception as e:
            logger.error(f"[{self.name}] Failed to send message: {e}")
            return False
            
    async def safe_save_price(self, **kwargs):
        """Save price with retries and connection management"""
        retries = 3
        for attempt in range(retries):
            try:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        await self.save_price(**kwargs)
                return True
            except asyncpg.exceptions.ConnectionDoesNotExistError:
                logger.error(f"[{self.name}] DB connection lost, reinitializing pool")
                await self.init_pool()
            except Exception as e:
                logger.error(f"[{self.name}] Save error (attempt {attempt+1}/{retries}): {e}")
                await asyncio.sleep(1)
        return False
    
    @abstractmethod
    async def connect(self):
        pass
    
    @abstractmethod
    async def subscribe(self, ws, pairs):
        pass
    
    @abstractmethod
    async def process_message(self, msg):
        pass
    
    def standardize_pair(self, pair: str, exchange: str) -> str:
        """
        Standardize the currency pair format to 'BASE-QUOTE'.
        Handles different separators and quote currencies.
        """
        # Define known quote currencies
        known_quotes = ['USDT', 'USD', 'EUR']

        # Detect separator
        separator = '-' if '-' in pair else '/' if '/' in pair else None

        if separator:
            base, quote = pair.split(separator)
        else:
            # Assume no separator means the last known quote is the quote currency
            for quote in known_quotes:
                if pair.endswith(quote):
                    base = pair[:-len(quote)]
                    break
            else:
                # Default to treating the last 3 characters as the quote
                base, quote = pair[:-3], pair[-3:]

        # Standardize separator to '-'
        return f"{base}-{quote}"

    async def save_price(self, pair, price, volume_24h, vwap_24h, high_24h, low_24h, trades_24h, opening_price):
        try:
            standardized_pair = self.standardize_pair(pair, self.name)
            now = time.time()
            last_update = self.last_updates.get(standardized_pair, 0)
            
            # Determine update frequency based on volume
            if volume_24h > 1000000:  # High volume
                threshold = self.update_thresholds['high']
            elif volume_24h > 100000:  # Medium volume
                threshold = self.update_thresholds['medium']
            else:  # Low volume
                threshold = self.update_thresholds['low']

            # Skip if updated too recently
            if now - last_update < threshold:
                return

            self.last_updates[standardized_pair] = now
            
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO prices (
                        time, pair, price, volume_24h, vwap_24h, 
                        high_24h, low_24h, trades_24h, opening_price, exchange
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ''',
                    datetime.utcnow(),
                    standardized_pair,
                    price,
                    volume_24h,
                    vwap_24h,
                    high_24h,
                    low_24h,
                    trades_24h,
                    opening_price,
                    self.name
                )

        except Exception as e:
            logger.error(f"[{self.name}] DB Error saving {pair}: {str(e)}")
    
    async def ping_loop(self, ws):
        """Generic ping/pong handler"""
        while True:
            try:
                await asyncio.sleep(15)  # Send ping every 15 seconds
                await ws.ping()
            except Exception as e:
                logger.error(f"[{self.name}] Ping error: {e}")
                break
    
    async def log_error(self, error: Exception, context: str):
        """Log error with context and update metrics"""
        self.error_count += 1
        self.last_error = {
            'time': datetime.utcnow(),
            'error': str(error),
            'context': context
        }
        logger.error(f"[{self.name}] {context}: {error}", exc_info=True)
    
    async def check_connection_health(self, ws):
        """Check connection health based on multiple criteria"""
        now = time.time()
        
        # Check connection age
        if now - self.connection_start_time > self.MAX_CONNECTION_TIME:
            logger.warning(f"[{self.name}] Connection too old, reconnecting")
            await ws.close()
            return False
            
        # Check for stale connection
        if now - self.last_message_time > self.STALE_CONNECTION_TIMEOUT:
            logger.warning(f"[{self.name}] Connection stale (no messages for {self.STALE_CONNECTION_TIMEOUT}s)")
            await ws.close()
            return False
            
        return True
    
    async def connection_health_loop(self, ws):
        """Periodically check connection health"""
        while True:
            await asyncio.sleep(30)  # Check every 30 seconds
            if not await self.check_connection_health(ws):
                break
    
    async def cleanup(self):
        async with self._pool_lock:
            if self.pool:
                await self.pool.close()
                self.pool = None
    
    async def log_status(self):
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO exchange_status_history (
                        exchange, status, active_pairs,
                        error_count, last_message_time, connection_duration
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                """,
                    self.name,
                    'CONNECTED' if time.time() - self.last_message_time < 30 else 'STALE',
                    len(getattr(self, 'pairs', [])),  # Safely get pairs if they exist
                    self.error_count,
                    datetime.fromtimestamp(self.last_message_time),
                    int(time.time() - self.connection_start_time)
                )
        except Exception as e:
            logger.error(f"Failed to log status: {e}")
    
    async def log_connection_event(self, event_type, error_details=None):
        metrics = {
            'cpu_percent': psutil.cpu_percent(),
            'memory_percent': psutil.virtual_memory().percent,
            'network': {
                interface: {
                    'isup': stats.isup,
                    'speed': stats.speed
                }
                for interface, stats in psutil.net_if_stats().items()
            }
        }
        
        connection_data = {
            'uptime': time.time() - self.connection_start_time,
            'messages_received': getattr(self, 'message_count', 0),
            'last_message_age': time.time() - self.last_message_time,
            'error_count': self.error_count,
            'host': socket.gethostname()
        }

        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO connection_events 
                (exchange, event_type, duration_seconds, error_details, 
                connection_metadata, system_metrics)
                VALUES ($1, $2, $3, $4, $5, $6)
            """,
                self.name,
                event_type,
                int(time.time() - self.connection_start_time),
                error_details,
                json.dumps(connection_data),
                json.dumps(metrics)
            )

    async def handle_api_error(self, error_type, error_msg, context=None, raw_error=None):
        try:
            # Store error in memory
            self._pending_errors.append({
                'exchange': self.name,
                'error_type': error_type,
                'error_msg': error_msg,
                'raw_error': str(raw_error) if raw_error else None,
                'context': context
            })
            
            # Only write to DB if we have enough errors or enough time passed
            if len(self._pending_errors) >= 10 or (time.time() - self._last_error_flush) > 60:
                async with self.pool.acquire() as conn:
                    await conn.executemany("""
                        INSERT INTO exchange_errors 
                        (exchange, error_type, error_message, context, time)
                        VALUES ($1, $2, $3, $4, $5)
                    """, [(e['exchange'], e['error_type'], e['error_msg'], 
                          json.dumps(e['context']) if e['context'] else None,
                          e['timestamp']) 
                         for e in self._pending_errors])
                self._pending_errors = []
                self._last_error_flush = time.time()
        except Exception as e:
            logger.error(f"Failed to log error batch: {e}")

    async def health_check_loop(self, ws):
        """Continuous health monitoring"""
        conn = await self.pool.acquire()
        try:
            while True:
                await asyncio.sleep(30)  # Increased from 5s to 30s
                now = time.time()
                
                if now - self.last_message_time > self.STALE_CONNECTION_TIMEOUT:
                    await conn.execute("""
                        INSERT INTO exchange_errors 
                        (exchange, error_type, error_message, context)
                        VALUES ($1, $2, $3, $4)
                    """,
                    self.name,
                    'STALE_CONNECTION',
                    'No messages received within timeout period',
                    json.dumps({
                        'last_message': self.last_message_time,
                        'timeout_threshold': self.STALE_CONNECTION_TIMEOUT,
                        'time_since_last': now - self.last_message_time
                    }))
                    
                    await ws.close()
                    break
        finally:
            await self.pool.release(conn)
