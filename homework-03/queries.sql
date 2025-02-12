-- Create an external table from parquet files
CREATE OR REPLACE EXTERNAL TABLE `folkloric-stone-449913-n7.de_zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-zoomcamp_hw3/yellow_tripdata_2024-*.parquet']
);

-- Create a (regular/materialized) table from parquet files
LOAD DATA OVERWRITE `folkloric-stone-449913-n7.de_zoomcamp.regular_yellow_tripdata`
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://de-zoomcamp_hw3/yellow_tripdata_2024-*.parquet']
);

-- QUESTION 1
-- What is count of records for the 2024 Yellow Taxi Data?
SELECT
  COUNT(*) cnt
FROM `folkloric-stone-449913-n7.de_zoomcamp.regular_yellow_tripdata`;
-- Answer: 20332093

-- QUESTION 2
-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT
  COUNT(DISTINCT PULocationID) cnt
FROM `folkloric-stone-449913-n7.de_zoomcamp.regular_yellow_tripdata`;
-- This query will process 155.12 MB when run.

SELECT
  COUNT(DISTINCT PULocationID) cnt
FROM `folkloric-stone-449913-n7.de_zoomcamp.external_yellow_tripdata`;
-- This query will process 0 B when run.
-- Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table

-- QUESTION 3
-- Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. 
-- Now write a query to retrieve the PULocationID and DOLocationID on the same table. 
-- Why are the estimated number of Bytes different?
SELECT
  PULocationID
FROM `folkloric-stone-449913-n7.de_zoomcamp.regular_yellow_tripdata`;
-- This query will process 155.12 MB when run.

SELECT
  PULocationID
  ,DOLocationID
FROM `folkloric-stone-449913-n7.de_zoomcamp.regular_yellow_tripdata`;
-- This query will process 310.24 MB when run.
-- Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. 
-- Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), 
-- leading to a higher estimated number of bytes processed.

-- QUESTION 4
-- How many records have a fare_amount of 0?
SELECT
  COUNT(*) cnt
FROM `folkloric-stone-449913-n7.de_zoomcamp.regular_yellow_tripdata`
WHERE fare_amount = 0;
-- Answer: 8333

-- QUESTION 5
-- What is the best strategy to make an optimized table in Big Query if your query will always 
-- filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
LOAD DATA OVERWRITE `folkloric-stone-449913-n7.de_zoomcamp.partitioned_yellow_tripdata`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://de-zoomcamp_hw3/yellow_tripdata_2024-*.parquet']
);
-- Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID

-- QUESTION 6
-- Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. 
-- Now change the table in the from clause to the partitioned table you created for question 5 
-- and note the estimated bytes processed. What are these values?
SELECT DISTINCT 
  VendorID
FROM `folkloric-stone-449913-n7.de_zoomcamp.regular_yellow_tripdata`
WHERE tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime < '2024-03-16';
-- This query will process 310.24 MB when run.

SELECT DISTINCT 
  VendorID
FROM `folkloric-stone-449913-n7.de_zoomcamp.partitioned_yellow_tripdata`
WHERE tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime < '2024-03-16';
-- This query will process 26.84 MB when run.
-- Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
