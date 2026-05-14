-- migrations/001_create_tables.up.sql
-- Apply: create tables for PostgreSQL with BIGINT ids
CREATE TABLE metrics (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    host TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    cpu_pct REAL NOT NULL,
    ram_pct REAL NOT NULL,
    ram_free REAL NOT NULL,
    net_sent REAL NOT NULL,
    net_recv REAL NOT NULL,
    disk_pct REAL NOT NULL,
    disk_free REAL NOT NULL,
    net_sent_mbps REAL NOT NULL DEFAULT 0,
    net_recv_mbps REAL NOT NULL DEFAULT 0,
    disk_read_mbps REAL NOT NULL DEFAULT 0,
    disk_write_mbps REAL NOT NULL DEFAULT 0
);

CREATE INDEX idx_metrics_host_ts ON metrics(host, ts);

CREATE TABLE speedtest (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    host TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    download_mbps REAL NOT NULL,
    upload_mbps REAL NOT NULL,
    ping_ms REAL NOT NULL,
    server TEXT NOT NULL
);

CREATE INDEX idx_speedtest_host_ts ON speedtest(host, ts);

CREATE TABLE rules (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    host TEXT NOT NULL DEFAULT 'my-pc',
    metric TEXT NOT NULL,
    threshold REAL NOT NULL,
    count INTEGER NOT NULL DEFAULT 3,
    enabled BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_rules_host_metric ON rules(host, metric);

CREATE TABLE mutes (
    host TEXT PRIMARY KEY,
    muted_until TIMESTAMPTZ NOT NULL
);

CREATE TABLE incidents (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    host TEXT NOT NULL,
    metric TEXT NOT NULL,
    value REAL NOT NULL,
    threshold REAL NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    notified BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_incidents_host_metric ON incidents(host, metric);
CREATE INDEX idx_incidents_started_at ON incidents(started_at);