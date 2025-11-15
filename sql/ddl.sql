CREATE OR REPLACE TABLE staging.nyc_tlc_tripdata_local (
    driver_id UInt32,                             -- сгенерированный ID водителя
    pickup_datetime DateTime64(3, 'UTC'),         -- время начала поездки
    dropoff_datetime DateTime64(3, 'UTC'),        -- время конца поездки
    passenger_count Nullable(UInt8),              -- кол-во пассажиров, может быть пустое
    trip_distance Float32,                        -- расстояние поездки
    pu_location_id UInt32,                         -- ID зоны посадки
    do_location_id UInt32,                         -- ID зоны высадки
    fare_amount Float32,                           -- стоимость поездки
    total_amount Float32,                          -- общая сумма с чаевыми и сборами
    cab_type String,                               -- "yellow" / "green"
    vendor_id UInt32,                              -- ID компании-владельца
    store_and_fwd_flag Nullable(String),          -- флаг сохранения и пересылки данных
    ratecode_id UInt32,                            -- код тарифа
    extra Float32,                                 -- дополнительные сборы
    mta_tax Float32,                               -- налог MTA
    tip_amount Float32,                            -- чаевые
    tolls_amount Float32,                           -- сборы за платные дороги
    ehail_fee Float32,                             -- сбор за электронный заказ (может быть 0)
    improvement_surcharge Float32,                 -- дополнительная плата
    payment_type UInt8,                            -- тип оплаты (нал, карта, прочее)
    trip_type UInt8,                               -- тип поездки
    congestion_surcharge Nullable(Float32),       -- плата за пробки, может быть пустой
    cbd_congestion_fee Float32,                   -- плата за въезд в центр
    _etl_timestamp DateTime DEFAULT now()         -- время вставки в таблицу
)
ENGINE = ReplacingMergeTree(_etl_timestamp)       -- позволяет заменять дубликаты
PARTITION BY toYYYYMM(pickup_datetime)           -- партицирование по месяцу
ORDER BY (driver_id, pickup_datetime)           -- сортировка для оптимизации по водителю и времени
SETTINGS index_granularity = 8192;
