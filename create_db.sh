#!/bin/bash
USERNAME: 'postgres'
DB_NAME: 'store_monitoring'






CREATE TABLE store_business_hours (
    store_id INTEGER,
    day INTEGER,
    start_time_local TIME,
    end_time_local TIME
);


CREATE TABLE store_timezones (
    store_id INTEGER,
    timezone_str VARCHAR(50)
);


CREATE TABLE store_activity (
    store_id NUMERIC,
    status VARCHAR(8),
    timestamp_utc TIMESTAMP
    
);

\copy store_activity (store_id, status, timestamp_utc) FROM '/tmp/store_activity.csv' CSV HEADER;


\copy store_business_hours (store_id, day, start_time_local, end_time_local) FROM '/tmp/store_business_hours.csv' CSV HEADER;


\copy store_timezones (store_id, timezone_str) FROM '/tmp/store_timezones.csv' CSV HEADER;


ALTER TABLE store_business_hours
ALTER COLUMN store_id SET DATA TYPE numeric;


create table for storing the generated report data:

CREATE TABLE reports (
    report_id UUID,
    store_id NUMERIC,
    uptime_last_hour INTEGER,
    uptime_last_day INTEGER,
    uptime_last_week INTEGER,
    downtime_last_hour INTEGER,
    downtime_last_day INTEGER,
    downtime_last_week INTEGER
);


