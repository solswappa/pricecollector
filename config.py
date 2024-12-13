from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DB_HOST: str
    DB_NAME: str = "crypto_prices"
    DB_USER: str
    DB_PASS: str
    LOG_LEVEL: str = "INFO"
    COLLECTION_INTERVAL: int = 900
    ENABLED_EXCHANGES: list[str] = [
        "BybitExchange",
        "KrakenExchange",
        "BinanceExchange",
        "CoinbaseExchange",
        "OKXExchange"
    ]
    API_KEY: str

    # Connection settings
    DB_POOL_MIN_SIZE: int = 2
    DB_POOL_MAX_SIZE: int = 10
    WS_RECONNECT_DELAY: int = 5
    WS_PING_INTERVAL: int = 30
    WS_TIMEOUT: int = 60
    MAX_PAIRS_PER_EXCHANGE: int = 50

    class Config:
        env_file = ".env"

settings = Settings() 