-- gold.scenario_projections
-- Written by: Scenario Agent (A-04)
-- Grain: one row per (run_id, scenario_name)
-- Contains Monte Carlo P10/P50/P90 cost and timeline distributions for
-- 3 response strategies: do_nothing, partial_remediation, full_recall.
-- Pre-computed parameter variants for Tableau parameter actions (POC-3).

{{ config(materialized='table', schema='gold') }}

WITH base AS (
    SELECT *
    FROM gold_raw.scenario_projections_raw
),

enriched AS (
    SELECT
        sp.run_id,
        sp.scenario_name,           -- 'do_nothing' | 'partial_remediation' | 'full_recall'
        sp.is_recommended,          -- boolean: lowest expected cost scenario
        sp.confidence_flag,         -- 'OK' | 'LOW_CONFIDENCE' (AC-22 CI overlap check)

        -- Cost distribution (USD) — Monte Carlo outputs
        ROUND(sp.mean_total_cost_usd, 0)   AS mean_total_cost_usd,
        ROUND(sp.p10_total_cost_usd,  0)   AS p10_total_cost_usd,
        ROUND(sp.p50_total_cost_usd,  0)   AS p50_total_cost_usd,
        ROUND(sp.p90_total_cost_usd,  0)   AS p90_total_cost_usd,

        -- Timeline distribution (days)
        ROUND(sp.mean_timeline_days, 1)    AS mean_timeline_days,
        ROUND(sp.p10_timeline_days,  1)    AS p10_timeline_days,
        ROUND(sp.p90_timeline_days,  1)    AS p90_timeline_days,

        -- Volume estimates
        ROUND(sp.mean_returns_volume, 0)   AS mean_returns_volume,
        ROUND(sp.mean_units_affected, 0)   AS mean_units_affected,

        -- Sensitivity analysis (AC-20)
        sp.most_sensitive_param,
        sp.sensitivity_rank,

        -- Parameter variant metadata (POC-3 Tableau sliders)
        sp.return_rate_assumption,  -- the multiplier used for this pre-computed variant
        sp.param_overrides_json,    -- full override dict for traceability

        -- Cost range width (P90-P10) — uncertainty indicator for dashboard
        ROUND(sp.p90_total_cost_usd - sp.p10_total_cost_usd, 0)  AS cost_uncertainty_usd,

        sp.computed_at
    FROM base sp
)

SELECT * FROM enriched
