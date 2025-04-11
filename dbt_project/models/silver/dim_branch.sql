-- models/silver/dim_branches.sql
-- Description: Creates a dimension table for unique hospital branches.
-- Input: models/bronze/stg_departments.sql (or relevant staging model)
-- Output: Table in SILVER schema named dim_branches

WITH source_departments AS (
    -- Select branch information from the departments staging table,
    -- as it contains the Branch ID, Name, and State together.
    SELECT
        branch_id,
        branch_name,
        state_name,
        _loaded_at -- Timestamp from the staging load
    FROM {{ ref('stg_departments') }}
    WHERE branch_id IS NOT NULL -- Ensure we have a valid key
),
unique_branches AS (
    -- Deduplicate to get only one record per branch_id.
    -- Use ROW_NUMBER() in case there are multiple departments processed
    -- at slightly different times for the same branch (unlikely here, but safe).
    SELECT
        branch_id,
        branch_name,
        state_name,
        _loaded_at,
        ROW_NUMBER() OVER (PARTITION BY branch_id ORDER BY _loaded_at DESC) as rn
    FROM source_departments
)
SELECT
    -- Use the source branch_id as the primary key for the dimension
    branch_id,

    -- Cleaned Attributes
    TRIM(branch_name) AS branch_name,
    TRIM(state_name) AS state_name,

    -- Metadata
    _loaded_at

FROM unique_branches
WHERE rn = 1 -- Select only the most recent record for each branch_id