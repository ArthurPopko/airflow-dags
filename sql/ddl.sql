CREATE DATABASE staging;

CREATE OR REPLACE TABLE staging.nyc_tlc_tripdata_local (
    cab_type String,
    pickup_datetime DateTime64 (3, 'UTC'),
    dropoff_datetime DateTime64 (3, 'UTC'),
    driver_id UInt32,
    passenger_count UInt8,
    trip_distance Float32,
    PULocationID UInt16,
    DOLocationID UInt16,
    fare_amount Float32,
    total_amount Float32,
    trip_time_min Float32,
    _etl_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree
PARTITION BY
    toYYYYMM (pickup_datetime)
ORDER BY (
        cab_type, pickup_datetime
    ) SETTINGS index_granularity = 8192;