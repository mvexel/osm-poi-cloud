-- Create Athena database and table for POI data
-- Run this in the Athena console or via AWS CLI

-- Create database
CREATE DATABASE IF NOT EXISTS osm_pois;

-- Create external table pointing to Parquet files in S3
-- The table uses lon_bucket and lat_bucket columns for efficient bbox queries
CREATE EXTERNAL TABLE IF NOT EXISTS osm_pois.pois (
    osm_id STRING,
    osm_type STRING,
    name STRING,
    class STRING,
    lon DOUBLE,
    lat DOUBLE,
    state STRING,
    amenity STRING,
    shop STRING,
    leisure STRING,
    tourism STRING,
    cuisine STRING,
    opening_hours STRING,
    phone STRING,
    website STRING,
    brand STRING,
    operator STRING,
    tags STRING,
    lon_bucket INT,
    lat_bucket INT
)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/parquet/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Example bbox query that uses partition pruning:
-- SELECT * FROM osm_pois.pois
-- WHERE lon_bucket BETWEEN -123 AND -122
--   AND lat_bucket BETWEEN 37 AND 38
--   AND lon BETWEEN -122.5 AND -122.3
--   AND lat BETWEEN 37.7 AND 37.9;
