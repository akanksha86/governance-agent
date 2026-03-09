-- Create dataset for metadata changes if it doesn't exist
CREATE SCHEMA IF NOT EXISTS `governance_export`
OPTIONS(
  location="europe-west1"
);

-- Create table for metadata changes
CREATE OR REPLACE TABLE `governance_export.metadata_changes` (
  event_timestamp TIMESTAMP,
  entry_name STRING,
  entry_fqn STRING,
  change_type STRING,
  entry_type STRING,
  changed_aspects ARRAY<STRING>,
  metadata_snapshot STRING, -- Store as JSON string for now, or JSON type if preferred
  user_email STRING,
  summary STRING
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY entry_fqn, change_type;
