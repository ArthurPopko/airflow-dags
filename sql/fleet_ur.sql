select    vendor_id as fleet_id
        , driver_id
        , date_trunc ('day', pickup_datetime) as day
        , sum(datediff('second', pickup_datetime, dropoff_datetime)) / 3600 as trip_hours
from      staging.nyc_tlc_tripdata_local final
group by  fleet_id
        , driver_id
        , day;
