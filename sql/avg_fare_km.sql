with 1.60934 as miles_to_km
select    vendor_id as fleet_id
        , driver_id
        , sum(trip_distance) * miles_to_km as total_distance
        , sum(fare_amount) as total_fare
from      staging.nyc_tlc_tripdata_local
group by  fleet_id
        , driver_id;