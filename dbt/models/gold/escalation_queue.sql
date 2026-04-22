-- gold.escalation_queue
-- Written by: all agents (A-01 through A-05)
-- Grain: one row per escalation event
-- Human-in-the-loop queue. Analysts resolve items here; resolved=TRUE removes
-- them from active Tableau alerts. Demonstrates AC-07, AC-13, AC-18, AC-26.

{{ config(materialized='table', schema='gold') }}

WITH base AS (
    SELECT *
    FROM gold_raw.escalation_queue_raw
),

enriched AS (
    SELECT
        eq.escalation_id,
        eq.run_id,
        eq.agent_id,               -- 'A-01' | 'A-02' | 'A-03' | 'A-04' | 'A-05'
        eq.question,               -- human-readable question requiring analyst input
        eq.context_json,           -- structured context for analyst

        -- Resolution tracking
        eq.resolved,               -- boolean
        eq.resolved_by,            -- analyst name/email who resolved
        eq.resolution_notes,       -- free-text analyst notes
        eq.resolved_at,

        -- Urgency derived from agent type and context
        CASE
            WHEN eq.agent_id = 'A-03'
             AND CAST(GET_JSON_OBJECT(eq.context_json, '$.confidence') AS DOUBLE) < 0.50
                THEN 'HIGH'
            WHEN eq.agent_id IN ('A-01', 'A-02')
                THEN 'MEDIUM'
            ELSE 'LOW'
        END                        AS urgency,

        -- Age of open escalation in hours (for SLA tracking)
        CASE
            WHEN eq.resolved = FALSE THEN
                ROUND(
                    (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(eq.escalated_at))
                    / 3600,
                    1
                )
            ELSE NULL
        END                        AS open_hours,

        eq.escalated_at
    FROM base eq
)

SELECT * FROM enriched
