## clean trip data
CREATE TABLE staging.nyc_tlc_tripdata_local (
    driver_id UInt32, --     сгенерированный ID водителя
    pickup_datetime DateTime64 (3, 'UTC'), -- время начала поездки
    dropoff_datetime DateTime64 (3, 'UTC'), -- время конца поездки
    passenger_count Nullable (UInt8), -- кол-во пассажиров, может быть пустое
    trip_distance Float32, --    расстояние поездки
    pu_location_id UInt32, --    ID зоны посадки
    do_location_id UInt32, --    ID зоны высадки
    fare_amount Float32, --  стоимость поездки
    total_amount Float32, --     общая сумма с чаевыми и сборами
    cab_type String, --  "yellow" / "green"
    vendor_id UInt32, --     ID компании-владельца
    store_and_fwd_flag Nullable (String), -- флаг сохранения и пересылки данных
    ratecode_id Nullable (UInt32), -- код тарифа
    extra Nullable (Float32), -- дополнительные сборы
    mta_tax Nullable (Float32), -- налог MTA
    tip_amount Nullable (Float32), -- чаевые
    tolls_amount Nullable (Float32), -- сборы за платные дороги
    ehail_fee Nullable (Float32), -- сбор за электронный заказ (может быть 0)
    improvement_surcharge Nullable (Float32), -- дополнительная плата
    payment_type Nullable (UInt8), -- тип оплаты (нал, карта, прочее)
    trip_type Nullable (UInt8), -- тип поездки
    congestion_surcharge Nullable (Float32), -- плата за пробки, может быть пустой
    cbd_congestion_fee Nullable (Float32), -- плата за въезд в центр
    _etl_timestamp DateTime DEFAULT now() -- время вставки в таблицу
) ENGINE = ReplacingMergeTree (_etl_timestamp) -- позволяет заменять дубликаты
PARTITION BY
    toYYYYMM (pickup_datetime) -- партицирование по месяцу
ORDER BY (driver_id, pickup_datetime) -- сортировка для оптимизации по водителю и времени
    SETTINGS index_granularity = 8192;

## driver_performance view
CREATE OR REPLACE VIEW pl.driver_performance AS
SELECT
    driver_id,
    count() AS total_trips,
    sum(
        dateDiff('second', pickup_datetime, dropoff_datetime)
    ) AS total_trip_seconds,
    (total_trip_seconds / (12 * 3600)) * 100 AS utilization_pct,
    sum(fare_amount) AS total_fare,
    sum(trip_distance) AS total_distance_km,
    total_fare / nullIf(total_distance_km, 0) AS avg_fare_per_km,
    (total_fare / nullIf(total_trip_seconds, 0)) * 60 AS avg_fare_per_min
FROM staging.nyc_tlc_tripdata_local
GROUP BY driver_id;
;

## trip_efficiency
CREATE OR REPLACE VIEW pl.trip_efficiency AS
SELECT
    driver_id,
    pickup_datetime,
    dropoff_datetime,
    trip_distance,
    fare_amount,
    dateDiff('minute', pickup_datetime, dropoff_datetime) AS trip_duration_min,
    trip_distance / nullIf(dateDiff('minute', pickup_datetime, dropoff_datetime), 0) AS speed_m_per_min,
    if(dateDiff('minute', pickup_datetime, dropoff_datetime) / nullIf(trip_distance, 0) > 5, 1, 0) AS is_anomaly
FROM staging.nyc_tlc_tripdata_local final
WHERE trip_distance > 0;

