-- count
select count() from staging.nyc_tlc_tripdata_local;

optimize table staging.nyc_tlc_tripdata_local;

TRUNCATE TABLE staging.nyc_tlc_tripdata_local;

select * from staging.nyc_tlc_tripdata_local limit 10;

SELECT
    driver_id,
    min(pickup_datetime) AS shift_start,
    max(dropoff_datetime) AS shift_end,
    sum(trip_time_min) AS total_trip_time_min,
    dateDiff(
        'minute',
        shift_start,
        shift_end
    ) AS shift_time_min,
    total_trip_time_min / shift_time_min AS utilization_rate
FROM staging.nyc_tlc_tripdata_local
GROUP BY
    driver_id;
;

select
    driver_id,
    sum(trip_distance * 1.60934) AS total_distance_km,
    sum(fare_amount) as total_fare,
    count() as trip_count
from staging.nyc_tlc_tripdata_local
group by
    driver_id
order by driver_id;

SELECT DISTINCT cab_type FROM staging.nyc_tlc_tripdata_local;