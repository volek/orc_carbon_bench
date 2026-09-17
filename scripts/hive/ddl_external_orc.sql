-- External ORC fact + dictionary over frozen BEST_ORC layout (P8).
-- Replace ${ORC_LOCATION} and ${DICTIONARY_LOCATION} before running.

CREATE DATABASE IF NOT EXISTS orc_bench;
USE orc_bench;

DROP TABLE IF EXISTS events_ext;
CREATE EXTERNAL TABLE events_ext (
  event_id STRING,
  user_id BIGINT,
  session_id STRING,
  country_code STRING,
  device_type STRING,
  status STRING,
  product_id BIGINT,
  campaign_id BIGINT,
  region_id INT,
  `timestamp` TIMESTAMP,
  amount DOUBLE,
  payload_json STRING,
  log_format STRING,
  log_message STRING
)
PARTITIONED BY (
  event_year INT,
  event_month INT,
  event_day INT
)
STORED AS ORC
LOCATION '${ORC_LOCATION}';

MSCK REPAIR TABLE events_ext;

DROP TABLE IF EXISTS dictionary_ext;
CREATE EXTERNAL TABLE dictionary_ext (
  product_id BIGINT,
  product_type STRING,
  product_name STRING
)
STORED AS ORC
LOCATION '${DICTIONARY_LOCATION}';

ANALYZE TABLE events_ext COMPUTE STATISTICS;
ANALYZE TABLE events_ext COMPUTE STATISTICS FOR COLUMNS
  event_id, user_id, product_id, campaign_id, status, country_code, log_format;
ANALYZE TABLE dictionary_ext COMPUTE STATISTICS;
