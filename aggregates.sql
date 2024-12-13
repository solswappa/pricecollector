-- Create 1 minute aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS prices_1min
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 minute', time) AS bucket,
    pair,
    exchange,
    first(price, time) as open,
    last(price, time) as close,
    max(price) as high,
    min(price) as low,
    avg(price) as vwap,
    sum(volume_24h) as volume
FROM prices
GROUP BY bucket, pair, exchange;

-- Add policy for 1min
SELECT add_continuous_aggregate_policy('prices_1min',
    start_offset => INTERVAL '3 minutes',
    end_offset => INTERVAL '0 minutes',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE);

-- Create 1 hour aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS prices_1hour
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', time) AS bucket,
    pair,
    exchange,
    first(price, time) as open,
    last(price, time) as close,
    max(price) as high,
    min(price) as low,
    avg(price) as vwap,
    sum(volume_24h) as volume
FROM prices
GROUP BY bucket, pair, exchange;

-- Add policy for 1hour
SELECT add_continuous_aggregate_policy('prices_1hour',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '0 hours',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE);

-- Create 1 day aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS prices_1day
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', time) AS bucket,
    pair,
    exchange,
    first(price, time) as open,
    last(price, time) as close,
    max(price) as high,
    min(price) as low,
    avg(price) as vwap,
    sum(volume_24h) as volume
FROM prices
GROUP BY bucket, pair, exchange;

-- Add policy for 1day
SELECT add_continuous_aggregate_policy('prices_1day',
    start_offset => INTERVAL '2 days',
    end_offset => INTERVAL '0 days',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE); 