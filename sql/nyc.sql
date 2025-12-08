-- count
select count() from staging.nyc_tlc_tripdata_local;

optimize table staging.nyc_tlc_tripdata_local FINAL DEDUPLICATE by driver_id,
pickup_datetime;


select fleet, count() from pl.fleet_metrics
where dropoff_datetime > 0 and pickup_datetime > 0
group by fleet;

truncate table staging.nyc_tlc_tripdata_local;