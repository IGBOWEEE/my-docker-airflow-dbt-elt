WITH source AS (
    SELECT * FROM {{  source('raw_hospital_data','drug_inventory')

}}   
)

SELECT
    "Drug Inventory ID" AS drug_inventory_id,
	"Drug Name" AS drug_name,
	"Quantity" AS quantity,
	"Unit Cost" AS unit_cost,
	"Expiry Date" AS expiry_date,
	"Snapshot Date" AS snapshot_date,
	"Branch Name" AS branch_name,
	"State" AS state_name,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM source