CREATE DATABASE IF NOT EXISTS staging;

CREATE OR REPLACE TABLE staging.nyc_tlc_tripdata_local (
    driver_id UInt32,
    pickup_datetime DateTime64(3, 'UTC'),
    dropoff_datetime DateTime64(3, 'UTC'),
    passenger_count Nullable(UInt8),
    trip_distance Float32,
    pu_location_id UInt32,
    do_location_id UInt32,
    fare_amount Float32,
    total_amount Float32,
    cab_type String,
    vendor_id UInt32,
    store_and_fwd_flag Nullable(String),
    ratecode_id UInt32,
    extra Float32,
    mta_tax Float32,
    tip_amount Float32,
    tolls_amount Float32,
    ehail_fee Float32,
    improvement_surcharge Float32,
    payment_type UInt8,
    trip_type UInt8,
    congestion_surcharge Nullable(Float32),
    cbd_congestion_fee Float32,
    _etl_timestamp DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_etl_timestamp)
PARTITION BY toYYYYMM(pickup_datetime)
ORDER BY (driver_id, pickup_datetime)
SETTINGS index_granularity = 8192;
