-- gold.executive_briefings
-- Written by: Summary Agent (A-05)
-- Grain: one row per run_id
-- Contains LLM-generated executive briefing text and fact-check audit results (AC-25).

{{ config(materialized='table', schema='gold') }}

WITH base AS (
    SELECT *
    FROM gold_raw.executive_briefings_raw
),

enriched AS (
    SELECT
        eb.run_id,
        eb.briefing_id,             -- UUID for this specific briefing

        -- LLM-generated content
        eb.briefing_text,           -- final self-corrected briefing (AC-25)
        eb.draft_text,              -- original pre-fact-check draft (for audit)
        eb.recommended_scenario,    -- from Scenario Agent (AC-23)
        eb.rca_confidence,          -- top confidence score from RCA findings

        -- Fact-check audit (AC-25 self-correction)
        eb.all_claims_verified,     -- boolean: TRUE if zero unverified claims
        eb.verified_claims,         -- JSON array of claims that passed tolerance check
        eb.unverified_claims,       -- JSON array of claims marked [⚠ UNVERIFIED]
        eb.word_count,

        -- Proactive trigger metadata (AC-24)
        eb.trigger_reason,          -- 'unit_threshold' | 'rate_threshold' | 'manual'
        eb.units_affected,
        eb.severity_rate,

        -- RCA escalation disclosure (AC-26)
        eb.rca_escalated,           -- boolean: TRUE if any RCA was ESCALATED status
        eb.escalation_note,         -- disclosure text appended to briefing

        -- Computed readability metric for dashboard
        LENGTH(eb.briefing_text)    AS briefing_char_count,
        SIZE(
            FROM_JSON(eb.unverified_claims, 'ARRAY<STRING>')
        )                           AS unverified_claim_count,

        eb.published_at
    FROM base eb
)

SELECT * FROM enriched
