CREATE TABLE ad_lakehouse.events (
    event_id    BIGINT,
    event_time  TIMESTAMP,
    user_id     STRING,
    amount      DOUBLE
)
PARTITIONED BY (day(event_time))
LOCATION 's3://s3-study-datalake/ad_lakehouse/events/'
TBLPROPERTIES (
    'table_type'     = 'ICEBERG',
    'format'         = 'parquet'
);

INSERT INTO ad_lakehouse.events VALUES
  (1, TIMESTAMP '2026-04-10 09:00:00', 'ul', 100.0), 
  (2, TIMESTAMP '2026-04-10 10:00:00', 'u2', 200.0);
INSERT INTO ad_lakehouse.events VALUES  
  (3, TIMESTAMP '2026-04-11 11:00:00', 'ul', 150.0);  
  
SELECT 
    committed_at, 
    snapshot_id, 
    parent_id, 
    operation, 
    manifest_list
FROM "ad_lakehouse"."events$snapshots"
ORDER BY committed_at DESC;

SELECT file_path, record_count, lower_bounds, upper_bounds
FROM "ad_lakehouse"."events$files";

SELECT 
    made_current_at, 
    snapshot_id, 
    parent_id, 
    is_current_ancestor
FROM "ad_lakehouse"."events$history"
ORDER BY made_current_at DESC;