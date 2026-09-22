-- External ORC fact + dictionary over frozen BEST_ORC layout (AUDEI-shaped audit).
-- Replace ${ORC_LOCATION} and ${DICTIONARY_LOCATION} before running.

CREATE DATABASE IF NOT EXISTS orc_bench;
USE orc_bench;

DROP TABLE IF EXISTS events_ext;
CREATE EXTERNAL TABLE events_ext (
  epk_id STRING,
  event_id STRING,
  event_ts TIMESTAMP,
  name STRING,
  channel_type STRING,
  state STRING,
  module STRING,
  session STRING,
  user_login STRING,
  payload_json STRING
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
  event_name STRING,
  event_family STRING
)
STORED AS ORC
LOCATION '${DICTIONARY_LOCATION}';

ANALYZE TABLE events_ext COMPUTE STATISTICS;
ANALYZE TABLE events_ext COMPUTE STATISTICS FOR COLUMNS
  epk_id, event_id, name, channel_type, state, module;
ANALYZE TABLE dictionary_ext COMPUTE STATISTICS;
