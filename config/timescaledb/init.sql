-- Schema for the Flink results. Idempotent: applied by the one-shot `db-setup` service on every
-- `docker compose up`, so tables added later also reach an existing database volume.
-- Timestamps of market data are local exchange time stored as if UTC (architecture §3.3).
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- PIPELINE=jdbc (JDBC sink test, bench/jdbc_sink_test.py): one row per symbol and 5-minute window.
-- Written with INSERT ... ON CONFLICT (symbol, window_end) DO UPDATE, so replays are idempotent.
-- `writes` counts how many times a row was written (1 = written once, >1 = replayed/re-run).
CREATE TABLE IF NOT EXISTS window_close (
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
SELECT create_hypertable('window_close', by_range('window_end', INTERVAL '1 day'), if_not_exists => TRUE);

-- PIPELINE=ema: Query 1 + 2 result per symbol and non-empty 5-minute window. Upserted on (symbol, window_end).
CREATE TABLE IF NOT EXISTS ema_window (
    symbol       text             NOT NULL,
    sec_type     text,
    window_start timestamptz      NOT NULL,
    window_end   timestamptz      NOT NULL,   -- also when the window is evaluated and an advisory is due (FR-8)
    close        double precision NOT NULL,   -- price of the latest event (by event time) in the window
    close_time   timestamptz      NOT NULL,   -- event time of that event
    ticks        bigint           NOT NULL,   -- usable price events in the window
    ema38        double precision NOT NULL,
    ema100       double precision NOT NULL,
    prev_ema38   double precision NOT NULL,   -- after the symbol's previous non-empty window (0 at the start)
    prev_ema100  double precision NOT NULL,
    advisory     text CHECK (advisory IN ('BUY', 'SELL')),
    initial      boolean          NOT NULL,   -- advisory caused by the EMA = 0 start value
    emitted_at   timestamptz      NOT NULL,   -- wall-clock time the Flink EMA function emitted the row
    written_at   timestamptz      NOT NULL DEFAULT now(),
    writes       integer          NOT NULL DEFAULT 1,
    PRIMARY KEY (symbol, window_end)
);
SELECT create_hypertable('ema_window', by_range('window_end', INTERVAL '1 day'), if_not_exists => TRUE);

CREATE OR REPLACE VIEW advisories AS
SELECT symbol, advisory AS type, window_start, window_end, window_end AS due_at, close, ema38, ema100,
       prev_ema38, prev_ema100, initial, emitted_at
FROM ema_window
WHERE advisory IS NOT NULL;

-- PIPELINE=ema: suspicious input data. One row per event (keyed by its Kafka partition and offset) or per window
-- (kafka_offset = -1), so re-processing after a restart adds nothing. Zero prices are only counted in the
-- Flink log (one per symbol and day), not stored here.
CREATE TABLE IF NOT EXISTS data_issue (
    reason         text             NOT NULL,   -- IssueType: MALFORMED, NEGATIVE_PRICE, LATE, TIME_SKEW, ...
    symbol         text             NOT NULL,
    event_time     timestamptz      NOT NULL,   -- the event's time, or the window end for window-level issues
    price          double precision NOT NULL,
    kafka_partition integer         NOT NULL,
    kafka_offset   bigint           NOT NULL,
    detail         text,
    detected_at    timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (reason, symbol, event_time, kafka_partition, kafka_offset)
);

CREATE OR REPLACE VIEW data_issue_summary AS
SELECT reason, count(*) AS events, count(DISTINCT symbol) AS symbols,
       min(event_time) AS first_event, max(event_time) AS last_event
FROM data_issue
GROUP BY reason
ORDER BY events DESC;
