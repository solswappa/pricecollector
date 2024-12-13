from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security.api_key import APIKeyHeader
from datetime import datetime
import asyncpg
from config import settings
from fastapi.responses import JSONResponse
import logging
from pydantic import BaseModel
from typing import Optional, List
import logging.config

logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "stream": "ext://sys.stdout"
        },
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": "DEBUG",
        }
    }
}

logging.config.dictConfig(logging_config)
logger = logging.getLogger(__name__)

app = FastAPI()
pool = None

api_key_header = APIKeyHeader(name="X-API-Key")

async def verify_api_key(api_key: str = Depends(api_key_header)):
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=403)
    return api_key

@app.on_event("startup")
async def startup():
    global pool
    try:
        logger.info("Connecting to database...")
        pool = await asyncpg.create_pool(
            database=settings.DB_NAME,
            host=settings.DB_HOST,
            user=settings.DB_USER,
            password=settings.DB_PASS,
            command_timeout=60,
            min_size=5,
            max_size=20
        )
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

@app.get("/status")
async def status():
    return {"status": "ok"}

@app.get("/health/exchanges")
async def get_exchange_health(api_key: str = Depends(verify_api_key)):
    try:
        async with pool.acquire() as conn:
            stats = await conn.fetch("""
                WITH exchange_stats AS (
                    SELECT 
                        exchange,
                        count(*) as updates_5min,
                        max(time) as last_update,
                        count(distinct pair) as active_pairs,
                        (SELECT count(*) 
                         FROM exchange_errors e 
                         WHERE e.exchange = p.exchange 
                         AND e.time > now() - interval '5 minutes') as recent_errors
                    FROM prices p
                    WHERE time > now() - interval '5 minutes'
                    GROUP BY exchange
                ),
                latest_errors AS (
                    SELECT DISTINCT ON (exchange) 
                        exchange,
                        error_type,
                        error_message,
                        context,
                        time as error_time
                    FROM exchange_errors
                    ORDER BY exchange, time DESC
                )
                SELECT 
                    e.exchange,
                    COALESCE(s.updates_5min, 0) as updates_5min,
                    COALESCE(s.active_pairs, 0) as active_pairs,
                    COALESCE(s.recent_errors, 0) as recent_errors,
                    EXTRACT(EPOCH FROM (now() - s.last_update))::decimal(15,3) as seconds_since_update,
                    CASE 
                        WHEN s.updates_5min = 0 THEN 'DISCONNECTED'
                        WHEN s.recent_errors > 10 THEN 'ERROR'
                        WHEN EXTRACT(EPOCH FROM (now() - s.last_update)) > 300 THEN 'STALE'
                        WHEN s.updates_5min < 10 THEN 'DEGRADED'
                        ELSE 'HEALTHY'
                    END as status,
                    err.error_type,
                    err.error_message,
                    err.context,
                    err.error_time
                FROM (SELECT DISTINCT exchange FROM prices) e
                LEFT JOIN exchange_stats s USING (exchange)
                LEFT JOIN latest_errors err USING (exchange)
                ORDER BY COALESCE(s.updates_5min, 0) DESC
            """)
            
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "exchanges": [dict(row) for row in stats]
            }
    except Exception as e:
        logger.error(f"Error in health check: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )

@app.get("/health/errors")
async def get_recent_errors(api_key: str = Depends(verify_api_key)):
    try:
        async with pool.acquire() as conn:
            errors = await conn.fetch("""
                SELECT 
                    time,
                    exchange,
                    error_type,
                    error_message,
                    context
                FROM exchange_errors
                ORDER BY time DESC
                LIMIT 100
            """)
            
            return [
                {
                    "time": row['time'].isoformat(),
                    "exchange": row['exchange'],
                    "error_type": row['error_type'],
                    "error_message": row['error_message'],
                    "context": row['context']
                } for row in errors
            ]
    except Exception as e:
        logger.error(f"Error fetching errors: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )

class PriceRequest(BaseModel):
    pairs: List[str]
    exchanges: Optional[List[str]] = None
    latest: bool = True
    interval: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: int = 1000

@app.post("/prices/batch")
async def get_batch_prices(request: PriceRequest):
    print("RAW REQUEST RECEIVED:", request)  # Basic print for immediate feedback
    logger.critical("Endpoint hit with request: %s", request)  # Highest log level
    
    try:
        logger.critical(f"Attempting database connection with pool: {pool}")
        async with pool.acquire() as conn:
            if request.latest:
                query = """
                    WITH latest_prices AS (
                        SELECT DISTINCT ON (exchange, pair) *
                        FROM prices
                        WHERE time > now() - interval '60 seconds'
                        AND pair = ANY($1)
                """
                
                params = [request.pairs]
                print("QUERY PARAMS:", params)  # Basic print
                logger.critical(f"Query parameters: {params}")
                
                if request.exchanges:
                    query += f" AND exchange = ANY(${len(params) + 1})"
                    params.append(request.exchanges)
                    
                query += """
                        ORDER BY exchange, pair, time DESC
                    )
                    SELECT * FROM latest_prices 
                    ORDER BY exchange, time DESC
                """
                logger.critical(f"Final query: {query}")
            
            else:
                # Always use raw prices table for now
                query = """
                    SELECT * FROM prices
                    WHERE pair = ANY($1)
                """
                params = [request.pairs]
                
                if request.exchanges:
                    query += f" AND exchange = ANY(${len(params) + 1})"
                    params.append(request.exchanges)
                
                if request.start_time:
                    query += f" AND time >= ${len(params) + 1}"
                    params.append(request.start_time)
                
                if request.end_time:
                    query += f" AND time <= ${len(params) + 1}"
                    params.append(request.end_time)
                
                query += " ORDER BY time DESC LIMIT $" + str(len(params) + 1)
                params.append(request.limit)
            
            results = await conn.fetch(query, *params)
            logger.critical(f"Query returned {len(results)} results")
            print("RESULTS:", results)  # Basic print
            
            grouped = {}
            exchanges_found = set()
            for row in results:
                pair = row['pair']
                if pair not in grouped:
                    grouped[pair] = []
                grouped[pair].append(dict(row))
                exchanges_found.add(row['exchange'])
            
            response = {
                "timestamp": datetime.utcnow().isoformat(),
                "pairs": grouped,
                "exchanges_found": list(exchanges_found),
                "total_pairs_found": len(grouped),
                "missing_pairs": [p for p in request.pairs if p not in grouped]
            }
            logger.info(f"Response summary: found {len(exchanges_found)} exchanges, {len(grouped)} pairs")
            return response

    except Exception as e:
        logger.error(f"Error fetching batch prices: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
    
class ChartRequest(BaseModel):
    pair: str
    interval: str = "1h"  # 1m, 5m, 15m, 1h, 4h, 1d
    exchange: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: int = 1000

@app.post("/prices/chart")
async def get_chart_data(request: ChartRequest, api_key: str = Depends(verify_api_key)):
    try:
        async with pool.acquire() as conn:
            table_name = {
                "1m": "prices_1min",
                "1h": "prices_1hour",
                "1d": "prices_1day"
            }.get(request.interval, "prices_1hour")

            query = f"""
                SELECT 
                    bucket as time,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM {table_name}
                WHERE pair = $1
                AND bucket > NOW() - INTERVAL '1 day'  -- Default time range
            """
            params = [request.pair]

            if request.exchange:
                query += " AND exchange = $2"
                params.append(request.exchange)

            if request.start_time:
                query += f" AND bucket >= ${len(params) + 1}"
                params.append(request.start_time)

            if request.end_time:
                query += f" AND bucket <= ${len(params) + 1}"
                params.append(request.end_time)

            query += " ORDER BY bucket DESC LIMIT $" + str(len(params) + 1)
            params.append(request.limit)

            results = await conn.fetch(query, *params)
            
            return {
                "pair": request.pair,
                "interval": request.interval,
                "points": len(results),
                "data": [dict(row) for row in results]
            }

    except Exception as e:
        logger.error(f"Error in chart data: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
    
@app.get("/pairs")
async def get_all_pairs(api_key: str = Depends(verify_api_key)):
    try:
        async with pool.acquire() as conn:
            pairs = await conn.fetch("""
                SELECT DISTINCT pair 
                FROM prices 
                ORDER BY pair
            """)
            
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "pairs": [row['pair'] for row in pairs],
                "total": len(pairs)
            }
    except Exception as e:
        logger.error(f"Error fetching pairs: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
    
    