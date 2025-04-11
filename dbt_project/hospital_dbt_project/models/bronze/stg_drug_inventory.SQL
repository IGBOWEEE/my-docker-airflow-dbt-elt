WITH source AS (
    SELECT * FROM {{  source('raw_hospital_data','drug_inventory')

}}   
)

SELECT
    "c1" AS drug_inventory_id,
	"c2" AS drug_name,
	"c3" AS quantity,
	"c4" AS unit_cost,
	"c5" AS expiry_date,
	"c6" AS snapshot_date,
	"c7" AS branch_name,
	"c8" AS state_name,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM source