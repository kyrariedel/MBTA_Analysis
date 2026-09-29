CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.travel_times (
    service_date DATE,
    route_id TEXT,
    from_stop_id TEXT,
    to_stop_id TEXT,
    dep_dt TIMESTAMP,
    arr_dt TIMESTAMP,
    travel_time_sec NUMERIC,
    benchmark_travel_time_sec NUMERIC
);

CREATE TABLE IF NOT EXISTS raw.headways (
    service_date DATE,
    route_id TEXT,
    stop_id TEXT,
    dep_dt TIMESTAMP,
    prev_dep_dt TIMESTAMP,
    headway_sec NUMERIC,
    benchmark_headway_sec NUMERIC
);

-- Completed files only. A second run appends new files and leaves loaded ones in place.
CREATE TABLE IF NOT EXISTS raw.ingest_log (
    source_file TEXT PRIMARY KEY,
    target_table TEXT NOT NULL,
    row_count BIGINT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.dim_stops (
    stop_id TEXT PRIMARY KEY,
    stop_name TEXT,
    route_id TEXT,
    latitude NUMERIC,
    longitude NUMERIC
);
