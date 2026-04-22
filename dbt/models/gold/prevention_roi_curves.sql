-- gold.prevention_roi_curves
-- 6-stage cumulative cost curves for 3 scenarios + AI investment line.
-- Powers Chart 4 (Prevention ROI).
-- Written by A-04 (scenario_agent.py).
-- Scenarios: detect_early | detect_late | do_nothing | ai_investment
{{ config(materialized='table', schema='gold') }}

SELECT
    scenario,
    CAST(stage_sequence AS INT)    AS stage_sequence,
    stage_name,
    CAST(stage_cost_usd       AS BIGINT) AS stage_cost_usd,
    CAST(cumulative_cost_usd  AS BIGINT) AS cumulative_cost_usd,
    CAST(p10_cumulative_usd   AS BIGINT) AS p10_cumulative_usd,
    CAST(p90_cumulative_usd   AS BIGINT) AS p90_cumulative_usd,
    CAST(savings_vs_detect_late AS BIGINT) AS savings_vs_detect_late,
    run_id,
    computed_at,
    created_at
FROM {{ source('gold_raw', 'prevention_roi_curves') }}
ORDER BY scenario, stage_sequence
