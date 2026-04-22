-- silver.supplier_defect_history
-- Source: supplier_defect_history.csv (ingested by A-01)
-- Grain: one row per (supplier_id, line_id, month)
-- Provides rolling historical defect rates used by Detection Agent's
-- baseline_lookup tool (AC-10) to calculate supplier-specific Z-scores.

{{ config(materialized='table', schema='silver') }}

WITH raw AS (
    SELECT *
    FROM bronze.supplier_defect_history
    WHERE _ingest_status = 'VALID'
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY supplier_id, line_id, month
               ORDER BY ingested_at DESC
           ) AS rn
    FROM raw
),

cleaned AS (
    SELECT
        UPPER(TRIM(supplier_id))               AS supplier_id,
        UPPER(TRIM(line_id))                   AS line_id,
        CAST(month AS DATE)                    AS month,          -- first day of month

        CAST(avg_defect_rate    AS DOUBLE)     AS avg_defect_rate,
        CAST(std_defect_rate    AS DOUBLE)     AS std_defect_rate,
        CAST(inspection_count   AS INT)        AS inspection_count,
        CAST(total_units        AS INT)        AS total_units,
        CAST(total_defects      AS INT)        AS total_defects,

        -- Trailing 3-month average (smoothed baseline used by Detection Agent)
        AVG(CAST(avg_defect_rate AS DOUBLE)) OVER (
            PARTITION BY supplier_id, line_id
            ORDER BY CAST(month AS DATE)
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )                                      AS trailing_3m_avg_rate,

        CAST(ingested_at AS TIMESTAMP)         AS ingested_at
    FROM deduped
    WHERE rn = 1
)

SELECT * FROM cleaned
