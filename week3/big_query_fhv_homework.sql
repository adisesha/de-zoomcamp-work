-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-338806.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_dtc-de-course-338806/csv/fhv_tripdata_2019-*.csv']
);

-- What is count for fhv vehicles data for year 2019? 
SELECT COUNT(1) FROM `dtc-de-course-338806.trips_data_all.external_fhv_tripdata`;

-- Question 2: How many distinct dispatching_base_num we have in fhv for 2019
SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `dtc-de-course-338806.trips_data_all.external_fhv_tripdata`;

-- Create table partitioned by dropoff_datetime and clustered by dispatching_base_num
CREATE OR REPLACE TABLE `dtc-de-course-338806.trips_data_all.fhb_tripdata_pby_dropoff_cby_base_num`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM `dtc-de-course-338806.trips_data_all.external_fhv_tripdata`;

-- Create table partitioned by pickup_datetime and clustered by dispatching_base_num
CREATE OR REPLACE TABLE `dtc-de-course-338806.trips_data_all.fhb_tripdata_pby_pickup_cby_base_num`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM `dtc-de-course-338806.trips_data_all.external_fhv_tripdata`;

-- Question 4: What is the count, estimated and actual data processed for query which counts 
-- trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279 
SELECT COUNT(*) FROM `dtc-de-course-338806.trips_data_all.fhb_tripdata_pby_pickup_cby_base_num` 
WHERE (pickup_datetime BETWEEN '2019-01-01' AND '2019-03-31') AND dispatching_base_num IN ('B00987','B02060', 'B02279');