-- gold.rca_findings
-- Written by: RCA Agent (A-03)
-- Grain: one row per (run_id, batch_id) investigation
-- Exposes top hypothesis, confidence, evidence sources, and structured hypotheses JSON.

{{ config(materialized='table', schema='gold') }}

WITH base AS (
    SELECT *
    FROM gold_raw.rca_findings_raw
),

enriched AS (
    SELECT
        rf.run_id,
        rf.batch_id,
        rf.supplier_id,

        -- RCA outcome
        rf.rca_status,            -- 'COMPLETE' | 'ESCALATED' | 'INSUFFICIENT_EVIDENCE'
        rf.top_hypothesis,
        ROUND(rf.top_confidence, 4)  AS top_confidence,
        rf.trigger_summary,           -- boolean: whether A-05 should be invoked

        -- Evidence and reasoning
        rf.evidence_sources,          -- JSON array of source names queried
        rf.hypotheses_json,           -- full ranked hypothesis list with confidence scores
        rf.data_gaps,                 -- JSON array of missing data that limited confidence

        -- Agentic pattern indicators (AC-15, AC-16, AC-17, AC-18)
        rf.tools_called,
        rf.defect_types_detected,     -- JSON array — drove source selection

        -- Join to batch/supplier for Tableau dims
        db.sku,
        db.production_date,
        db.line_id,
        db.lot_id,
        ds.supplier_name,
        ds.country,
        ds.tier,

        -- Confidence tier label for dashboard filter
        CASE
            WHEN rf.top_confidence >= 0.70 THEN 'HIGH'
            WHEN rf.top_confidence >= 0.50 THEN 'MEDIUM'
            ELSE 'LOW'
        END                           AS confidence_tier,

        rf.investigated_at
    FROM base rf
    LEFT JOIN silver.dim_batch    db ON rf.batch_id    = db.batch_id
    LEFT JOIN silver.dim_supplier ds ON rf.supplier_id = ds.supplier_id
)

SELECT * FROM enriched
