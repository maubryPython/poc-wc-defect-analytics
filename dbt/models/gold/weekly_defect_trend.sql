-- gold.weekly_defect_trend
-- Rolling weekly defect rate per supplier — powers Chart 1 (Detection Window).
-- Shows exactly which week the signal crossed the AI alert threshold (3.8%).
-- Written directly by A-02 (detection_agent.py); this dbt model validates/refreshes.
{{ config(materialized='table', schema='gold') }}

SELECT
    supplier_id,
    production_week,
    ROUND(weekly_defect_rate, 5)  AS weekly_defect_rate,
    total_defects,
    total_units_inspected,
    inspections,
    above_ai_threshold,
    ai_threshold,
    run_id,
    created_at
FROM {{ source('gold_raw', 'weekly_defect_trend') }}
ORDER BY supplier_id, production_week
