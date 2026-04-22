-- gold.run_log
-- Written by: Orchestrator (A-00)
-- Grain: one row per pipeline run
-- Tracks agent invocation sequence, tools called, total duration, and run outcome.
-- Used by Orchestrator "smart skip" logic (AC-05) and for demo traceability.

{{ config(materialized='table', schema='gold') }}

WITH base AS (
    SELECT *
    FROM gold_raw.run_log_raw
),

enriched AS (
    SELECT
        rl.run_id,
        rl.pipeline_state,         -- 'COMPLETE' | 'PARTIAL' | 'FAILED'

        -- Agent invocation sequence (AC-04 traceability)
        rl.agents_invoked,         -- JSON array, e.g. ["A-01","A-02","A-03","A-05"]
        rl.tools_called_count,     -- total tool calls across all agents
        rl.tools_called,           -- JSON array of all tool names used

        -- Routing decisions (AC-05 smart skip evidence)
        rl.skipped_agents,         -- JSON array of agents skipped with reasons
        rl.skip_reason,            -- human-readable explanation (e.g. "no new files")

        -- Timing
        CAST(rl.started_at   AS TIMESTAMP)  AS started_at,
        CAST(rl.completed_at AS TIMESTAMP)  AS completed_at,
        ROUND(
            (UNIX_TIMESTAMP(CAST(rl.completed_at AS TIMESTAMP))
             - UNIX_TIMESTAMP(CAST(rl.started_at  AS TIMESTAMP)))
            / 60,
            2
        )                          AS duration_minutes,

        -- Outcome counts
        rl.batches_ingested,
        rl.batches_flagged,
        rl.batches_investigated,
        rl.escalations_raised,

        -- Error tracking
        rl.errors,                 -- JSON array of error messages (empty on success)
        rl.retry_counts,           -- JSON object: {agent_id: retry_count}

        CAST(rl.started_at AS TIMESTAMP)    AS run_date
    FROM base rl
)

SELECT * FROM enriched
