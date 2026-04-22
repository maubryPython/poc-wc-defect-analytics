-- silver.supplier_risk_scores
-- Pre-production supplier risk assessment (5 dimensions + composite).
-- Source: supplier_risk_scores.csv ingested by A-01.
-- Drives Chart 2 (Supplier Risk Radar) and enriches A-02 alert thresholds.
{{ config(materialized='table', schema='silver') }}

SELECT
    supplier_id,
    supplier_name,
    region,
    material_type,
    contract_tier,
    CAST(historical_defect_score        AS DOUBLE) AS historical_defect_score,
    CAST(on_time_delivery_score         AS DOUBLE) AS on_time_delivery_score,
    CAST(audit_score                    AS DOUBLE) AS audit_score,
    CAST(material_lot_stability_score   AS DOUBLE) AS material_lot_stability_score,
    CAST(capacity_utilization_score     AS DOUBLE) AS capacity_utilization_score,
    CAST(composite_risk_score           AS DOUBLE) AS composite_risk_score,
    risk_tier,
    CAST(would_trigger_audit AS BOOLEAN) AS would_trigger_audit,
    CAST(scored_at          AS TIMESTAMP) AS scored_at,
    scoring_model_version,
    ingested_at,
    run_id
FROM {{ source('silver_raw', 'supplier_risk_scores') }}
