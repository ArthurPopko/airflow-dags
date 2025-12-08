select vendor_id as fleet_id
    , driver_id
    , date_trunc('day', pickup_datetime) as day
    , sum(dateDiff('second', pickup_datetime, dropoff_datetime))/3600 as trip_hours
    from staging.nyc_tlc_tripdata_local
    group by fleet_id, driver_id, day
    having trip_hours >= 0