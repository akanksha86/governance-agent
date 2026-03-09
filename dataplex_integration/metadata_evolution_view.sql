-- Create or replace a view to visualize metadata evolution
-- REPLACE placeholders below with your actual Project ID and Project Number
-- Project ID: e.g., my-project
-- Project Number: e.g., 1234567890

CREATE OR REPLACE VIEW `${PROJECT_ID}.governance_export.metadata_evolution` AS
WITH raw_data AS (
  SELECT 
    event_timestamp,
    entry_fqn,
    change_type,
    user_email,
    metadata_snapshot,
    -- Extract common fields from the JSON snapshot
    JSON_EXTRACT_SCALAR(metadata_snapshot, "$.entrySource.displayName") as display_name,
    JSON_EXTRACT(metadata_snapshot, "$.aspects") as all_aspects
  FROM `${PROJECT_ID}.governance_export.metadata_changes`
),
flattened_aspects AS (
  SELECT
    *,
    -- Extract specific core aspects using bracket notation for keys with dots
    -- In BigQuery JSONPath, dots in keys require bracket notation: $['key.with.dots']
    -- This uses the numerical project number which is required for custom aspects
    JSON_EXTRACT(all_aspects, "$['655216118709.global.schema']") as schema_aspect,
    JSON_EXTRACT(all_aspects, "$['${PROJECT_NUMBER}.europe-west1.data-governance-aspect']") as gov_aspect,
    -- Extract creation/update times from the entry itself (root of metadata_snapshot)
    JSON_EXTRACT_SCALAR(metadata_snapshot, "$.createTime") as entry_create_time,
    JSON_EXTRACT_SCALAR(metadata_snapshot, "$.updateTime") as entry_update_time
  FROM raw_data
)
SELECT 
  * EXCEPT(metadata_snapshot),
  -- Helper to extract column names for easy diffing in SQL if needed
  ARRAY(
    SELECT JSON_EXTRACT_SCALAR(field, "$.name")
    FROM UNNEST(JSON_EXTRACT_ARRAY(schema_aspect, "$.data.fields")) as field
  ) as columns
FROM flattened_aspects;
