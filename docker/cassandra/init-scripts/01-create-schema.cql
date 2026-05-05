-- Create keyspace
-- Using NetworkTopologyStrategy for distributed deployments
-- Replication factor of 3 for fault tolerance
CREATE KEYSPACE IF NOT EXISTS financial_data
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'dc1': 3
}
AND durable_writes = true;

-- Switch to the financial_data keyspace
USE financial_data;

-- Create table for minute-level OHLCV data
-- Partition key: symbol (asset identifier)
-- Clustering key: timestamp (for time-series ordering)
CREATE TABLE IF NOT EXISTS minute_data (
    symbol text,
    timestamp timestamp,
    open decimal,
    high decimal,
    low decimal,
    close decimal,
    volume bigint,
    adjusted_close decimal,
    PRIMARY KEY (symbol, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
AND comment = 'Minute-level OHLCV data for financial instruments'
AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': 1,
    'compaction_window_unit': 'DAYS'
}
AND gc_grace_seconds = 864000
AND default_time_to_live = 0;

-- Materialized view to get most recent prices efficiently
DROP MATERIALIZED VIEW IF EXISTS latest_prices;
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_prices AS
    SELECT symbol, timestamp, close, volume
    FROM minute_data
    WHERE symbol IS NOT NULL
      AND timestamp IS NOT NULL
    PRIMARY KEY ((symbol), timestamp)
    WITH CLUSTERING ORDER BY (timestamp DESC);

-- Create table for daily aggregated data
CREATE TABLE IF NOT EXISTS daily_data (
    symbol text,
    date date,
    open decimal,
    high decimal,
    low decimal,
    close decimal,
    volume bigint,
    avg_price decimal,
    vwap decimal,
    PRIMARY KEY (symbol, date)
) WITH CLUSTERING ORDER BY (date DESC)
AND comment = 'Daily aggregated OHLCV data';

-- Create table for metadata
CREATE TABLE IF NOT EXISTS asset_metadata (
    symbol text PRIMARY KEY,
    name text,
    sector text,
    industry text,
    market text,
    currency text,
    created_at timestamp,
    updated_at timestamp
) WITH comment = 'Metadata for financial assets';

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_sector ON asset_metadata(sector);
CREATE INDEX IF NOT EXISTS idx_industry ON asset_metadata(industry);
CREATE INDEX IF NOT EXISTS idx_market ON asset_metadata(market);

-- Create table for engineered features
CREATE TABLE IF NOT EXISTS engineered_features (
    symbol text,
    timestamp timestamp,
    returns decimal,
    log_returns decimal,
    sma_20 decimal,
    sma_50 decimal,
    sma_200 decimal,
    bb_middle decimal,
    bb_upper decimal,
    bb_lower decimal,
    volatility_20 decimal,
    PRIMARY KEY (symbol, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
AND comment = 'Technical indicators and engineered features';
