-- BRONZE: Streaming ingestion from sample taxi dataset
CREATE OR REFRESH STREAMING TABLE taxi_raw_records
(
  CONSTRAINT valid_distance EXPECT (trip_distance>0) ON VIOLATION DROP ROW
)
-- Drops rows where trip_distance <= 0
AS
SELECT *
FROM STREAM(samples.nyctaxi.trips);

-- SILVER 1: Suspicious ride detection
-- Flags rides where fare is unusually high
CREATE OR REFRESH STREAMING TABLE flagged_rides
AS
SELECT
  date_trunc("week",tpep_pickup_datetime) AS week,   
  pickup_zip AS zip,                                  
  fare_amount,  
  trip_distance
FROM STREAM(LIVE.taxi_raw_records)
WHERE 
    (pickup_zip = dropoff_zip AND fare_amount > 50)    
    OR (trip_distance < 5 AND fare_amount > 50);        -- very short distance but high fare

-- SILVER 2: Weekly aggregates for reporting
-- Computes weekly average fare and average distance
CREATE OR REFRESH MATERIALIZED VIEW weekly_stats
AS
SELECT
  date_trunc("week",tpep_pickup_datetime) AS week,
  AVG(fare_amount) AS avg_amount,
  AVG(trip_distance) AS avg_distance
FROM LIVE.taxi_raw_records
GROUP BY week
ORDER BY week ASC;

-- GOLD: Top 3 highest fare suspicious rides
-- Combines suspicious rides with weekly stats for business insights
CREATE OR REPLACE MATERIALIZED VIEW top_n
AS
SELECT
    ws.week,
    ROUND(ws.avg_amount, 2) AS avg_amount,      
    ROUND(ws.avg_distance, 3) AS avg_distance,  
    fr.fare_amount,                             
    fr.trip_distance,                           
    fr.zip AS passenger_area                    
FROM LIVE.flagged_rides fr
LEFT JOIN LIVE.weekly_stats ws
    ON ws.week = fr.week
ORDER BY fare_amount DESC                       
LIMIT 3;                                        
