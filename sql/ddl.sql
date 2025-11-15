CREATE DATABASE IF NOT EXISTS staging;

CREATE OR REPLACE TABLE staging.nyc_tlc_tripdata_local (
    driver_id UInt32,
    pu_location_id UInt32,
    do_location_id UInt32,
    vendor_id UInt32,
    ratecode_id UInt32,
    pickup_datetime DateTime64(3, 'UTC'),
    dropoff_datetime DateTime64(3, 'UTC'),
    trip_distance Float32,
    fare_amount Float32,
    total_amount Float32,
    extra Float32,
    mta_tax Float32,
    tip_amount Float32,
    tolls_amount Float32,
    ehail_fee Float32,
    improvement_surcharge Float32,
    congestion_surcharge Nullable(Float32),
    cbd_congestion_fee Float32,
    passenger_count Nullable(UInt8),
    payment_type UInt8,
    trip_type UInt8,
    cab_type String,
    store_and_fwd_flag Nullable(String),
    _etl_timestamp DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_etl_timestamp)
PARTITION BY toYYYYMM(pickup_datetime)
ORDER BY (driver_id, pickup_datetime)
SETTINGS index_granularity = 8192;
