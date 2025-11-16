-- count
select count() from staging.nyc_tlc_tripdata_local;

optimize table staging.nyc_tlc_tripdata_local FINAL DEDUPLICATE by driver_id,
pickup_datetime;

select * from staging.nyc_tlc_tripdata_local limit 10;
