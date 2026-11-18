-- Runs once, on the first start of an empty TimescaleDB volume.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- One row per symbol and 5-minute window. Written by the Flink JDBC sink with
-- INSERT ... ON CONFLICT (symbol, window_end) DO UPDATE, so replays are idempotent.
-- `writes` counts how many times a row was written (1 = written once, >1 = replayed/re-run).
CREATE TABLE window_close (
    symbol          text             NOT NULL,
    window_start    timestamptz      NOT NULL,
    window_end      timestamptz      NOT NULL,
    close           double precision NOT NULL,
    ticks           bigint           NOT NULL,
    last_event_time timestamptz      NOT NULL,
    written_at      timestamptz      NOT NULL DEFAULT now(),
    writes          integer          NOT NULL DEFAULT 1,
    PRIMARY KEY (symbol, window_end)   -- a unique key on a hypertable must include the time column
);
SELECT create_hypertable('window_close', by_range('window_end', INTERVAL '1 day'));
