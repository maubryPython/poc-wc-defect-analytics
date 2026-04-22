-- silver.material_lots
-- Material lot metadata with defect status and detection timing.
-- Source: material_lots.csv ingested by A-01.
-- Drives Chart 3 (Lot Traceability) via gold.lot_defect_traceability.
{{ config(materialized='table', schema='silver') }}

SELECT
    lot_id,
    supplier_id,
    material_type,
    CAST(entered_production_date        AS TIMESTAMP) AS entered_production_date,
    CAST(first_batch_production_date    AS DATE)      AS first_batch_production_date,
    adhesive_batch_id,
    CAST(batch_count   AS INT)    AS batch_count,
    CAST(total_units   AS BIGINT) AS total_units,
    CAST(is_defective  AS BOOLEAN) AS is_defective,
    defect_root_cause,
    CAST(hours_to_first_alert AS DOUBLE) AS hours_to_first_alert,
    CAST(detected_by_agent AS BOOLEAN)   AS detected_by_agent,
    detection_method,
    ingested_at,
    run_id
FROM {{ source('silver_raw', 'material_lots') }}
