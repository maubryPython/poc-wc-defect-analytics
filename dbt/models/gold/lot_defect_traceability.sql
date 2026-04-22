-- gold.lot_defect_traceability
-- Lot-level defect rates + metadata — powers Chart 3 (Lot Traceability Timeline).
-- Shows which lots are defective, when they entered production, and how many
-- hours it would take a lot-level monitoring agent to fire an alert.
-- Written by A-03 (rca_agent.py); enriched from silver.material_lots.
{{ config(materialized='table', schema='gold') }}

SELECT
    lot_id,
    supplier_id,
    material_type,
    CAST(entered_production_date AS TIMESTAMP) AS entered_production_date,
    ROUND(mean_defect_rate, 5)   AS mean_defect_rate,
    batch_count,
    batches_flagged,
    CAST(is_defective AS BOOLEAN) AS is_defective,
    CAST(hours_to_first_alert AS DOUBLE) AS hours_to_first_alert,
    defect_root_cause,
    rca_status,
    ROUND(rca_top_confidence, 4) AS rca_top_confidence,
    run_id,
    traced_at,
    created_at
FROM {{ source('gold_raw', 'lot_defect_traceability') }}
ORDER BY is_defective DESC, mean_defect_rate DESC
