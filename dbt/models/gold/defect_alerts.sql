-- gold.defect_alerts
-- Written by: Detection Agent (A-02)
-- Grain: one row per batch inspection run
-- Contains anomaly scores, alert status, and full reasoning chains.
-- This model is a VIEW over the agent-written Delta table — dbt adds
-- computed fields and enforces column contracts without rewriting agent output.

{{ config(materialized='table', schema='gold') }}

WITH base AS (
    SELECT *
    FROM gold_raw.defect_alerts_raw  -- agent writes here; we never overwrite
),

enriched AS (
    SELECT
        da.run_id,
        da.batch_id,
        da.supplier_id,
        da.line_id,

        -- Anomaly metrics
        ROUND(da.observed_defect_rate, 6)   AS observed_defect_rate,
        ROUND(da.baseline_mean_rate,   6)   AS baseline_mean_rate,
        ROUND(da.baseline_std_rate,    6)   AS baseline_std_rate,
        ROUND(da.anomaly_score,        3)   AS anomaly_score,       -- Z-score
        da.isolation_forest_score,

        -- Alert classification
        da.alert_status,          -- 'FLAGGED' | 'REVIEW' | 'CLEAR' | 'UNRESOLVED'
        da.confidence,

        -- Agentic reasoning chain (AC-12)
        da.reasoning_chain,
        da.tools_called,

        -- Material lot corroboration flag (borderline secondary lookup, AC-11)
        da.lot_corroborated,
        da.lot_id,

        -- Dimensions for Tableau filtering
        db.sku,
        db.production_date,
        db.unit_count,
        ds.supplier_name,
        ds.country,
        ds.tier,

        -- Deviation above normal expressed as a percentage of baseline
        CASE
            WHEN da.baseline_mean_rate > 0 THEN
                ROUND(
                    (da.observed_defect_rate - da.baseline_mean_rate)
                    / da.baseline_mean_rate * 100,
                    1
                )
            ELSE NULL
        END                                 AS pct_above_baseline,

        da.alerted_at
    FROM base da
    LEFT JOIN silver.dim_batch    db ON da.batch_id    = db.batch_id
    LEFT JOIN silver.dim_supplier ds ON da.supplier_id = ds.supplier_id
)

SELECT * FROM enriched
