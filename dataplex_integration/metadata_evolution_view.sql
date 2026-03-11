-- Create or replace a view to visualize metadata evolution
-- This version filters out "no-change" snapshots to reduce noise in the UI.

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
comparison AS (
  SELECT
    *,
    -- Get the aspects from the previous snapshot for this specific entry
    LAG(all_aspects) OVER (PARTITION BY entry_fqn ORDER BY event_timestamp ASC) as prev_all_aspects
  FROM raw_data
),
filtered_data AS (
  SELECT *
  FROM comparison
  -- Keep a version if:
  -- 1. It's the first version (prev_all_aspects is NULL)
  -- 2. OR the aspects/schema actually changed compared to the previous version
  -- 3. OR it's a structural change type (like CREATE or DELETE)
  WHERE prev_all_aspects IS NULL 
     OR TO_JSON_STRING(all_aspects) != TO_JSON_STRING(prev_all_aspects)
     OR change_type IN ('CREATE', 'DELETE')
),
flattened_aspects AS (
  SELECT
    *,
    -- Extract specific core aspects using bracket notation for keys with dots
    -- In BigQuery JSONPath, dots in keys require bracket notation: $['key.with.dots']
    JSON_EXTRACT(all_aspects, "$['655216118709.global.schema']") as schema_aspect,
    JSON_EXTRACT(all_aspects, "$['${PROJECT_NUMBER}.europe-west1.data-governance-aspect']") as gov_aspect,
    -- Extract creation/update times from the entry itself (root of metadata_snapshot)
    JSON_EXTRACT_SCALAR(metadata_snapshot, "$.createTime") as entry_create_time,
    JSON_EXTRACT_SCALAR(metadata_snapshot, "$.updateTime") as entry_update_time
  FROM filtered_data
)
SELECT 
  * EXCEPT(metadata_snapshot, prev_all_aspects),
  -- Helper to extract column names for easy diffing in SQL if needed
  ARRAY(
    SELECT JSON_EXTRACT_SCALAR(field, "$.name")
    FROM UNNEST(JSON_EXTRACT_ARRAY(schema_aspect, "$.data.fields")) as field
  ) as columns
FROM flattened_aspects;
