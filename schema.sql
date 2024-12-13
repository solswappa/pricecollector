-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Prices table for market data
CREATE TABLE IF NOT EXISTS prices (
    time TIMESTAMPTZ NOT NULL,
    pair TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    volume_24h BIGINT NOT NULL,
    vwap_24h DOUBLE PRECISION NOT NULL,
    high_24h DOUBLE PRECISION NOT NULL,
    low_24h DOUBLE PRECISION NOT NULL,
    trades_24h BIGINT NOT NULL,
    opening_price DOUBLE PRECISION NOT NULL,
    exchange TEXT NOT NULL DEFAULT 'KrakenExchange'
);

-- Create hypertable for time-series data
SELECT create_hypertable('prices', 'time', if_not_exists => TRUE);

-- Indexes for price queries
CREATE INDEX IF NOT EXISTS idx_prices_pair_time ON prices (pair, time DESC);
CREATE INDEX IF NOT EXISTS idx_prices_exchange_time ON prices (exchange, time DESC);
CREATE INDEX IF NOT EXISTS idx_prices_pair_exchange_time ON prices (pair, exchange, time DESC);

-- Error logging table
CREATE TABLE IF NOT EXISTS exchange_errors (
    id SERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    exchange TEXT NOT NULL,
    error_type TEXT NOT NULL,
    error_message TEXT,
    context JSONB
);

CREATE INDEX IF NOT EXISTS idx_errors_time_exchange 
ON exchange_errors (time DESC, exchange);

-- Exchange status tracking
CREATE TABLE IF NOT EXISTS exchange_status (
    id SERIAL PRIMARY KEY,
    exchange TEXT NOT NULL,
    status TEXT NOT NULL,
    last_update TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_message_time TIMESTAMPTZ,
    uptime_seconds INT,
    error_count INT DEFAULT 0,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_exchange_status_time 
ON exchange_status (last_update DESC);

-- Historical status tracking
CREATE TABLE IF NOT EXISTS exchange_status_history (
    id SERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    exchange TEXT NOT NULL,
    status TEXT NOT NULL,
    updates_count INT,
    active_pairs INT,
    error_count INT,
    last_message_time TIMESTAMPTZ,
    connection_duration INT,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_status_history_time 
ON exchange_status_history (time DESC, exchange);

-- Connection event logging
CREATE TABLE IF NOT EXISTS connection_events (
    id SERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    exchange TEXT NOT NULL,
    event_type TEXT NOT NULL, -- 'CONNECT', 'DISCONNECT', 'FAIL', 'RECONNECT'
    duration_seconds INT,
    error_details TEXT,
    connection_metadata JSONB, -- WebSocket details, connection stats
    system_metrics JSONB      -- CPU, memory, network stats at time of event
);

CREATE INDEX IF NOT EXISTS idx_connection_events_time 
ON connection_events (time DESC, exchange, event_type);

-- Add retention policies with error handling
DO $$
BEGIN
    PERFORM add_retention_policy('prices', INTERVAL '1 day', if_not_exists => TRUE);
    PERFORM add_retention_policy('prices_1min', INTERVAL '1 week', if_not_exists => TRUE);
    PERFORM add_retention_policy('prices_1hour', INTERVAL '3 months', if_not_exists => TRUE);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error creating retention policies: %', SQLERRM;
END $$;
  