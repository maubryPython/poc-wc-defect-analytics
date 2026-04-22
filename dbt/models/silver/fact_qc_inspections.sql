-- silver.fact_qc_inspections
-- Source: qc_inspection_logs.xlsx (ingested by A-01)
-- Grain: one row per inspection_id
-- Computes defect_rate = total_defects / units_inspected for use by Detection Agent (A-02).

{{ config(materialized='table', schema='silver') }}

WITH raw AS (
    SELECT *
    FROM bronze.qc_inspection_logs
    WHERE _ingest_status = 'VALID'
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY inspection_id
               ORDER BY ingested_at DESC
           ) AS rn
    FROM raw
),

cleaned AS (
    SELECT
        TRIM(inspection_id)                      AS inspection_id,
        UPPER(TRIM(batch_id))                    AS batch_id,
        UPPER(TRIM(supplier_id))                 AS supplier_id,
        UPPER(TRIM(line_id))                     AS line_id,
        CAST(inspection_date AS DATE)            AS inspection_date,
        CAST(units_inspected  AS INT)            AS units_inspected,
        CAST(total_defects    AS INT)            AS total_defects,

        -- Primary metric used by Detection Agent
        ROUND(
            CAST(total_defects AS DOUBLE) / NULLIF(units_inspected, 0),
            6
        )                                        AS defect_rate,

        -- Individual defect type counts (used by RCA source-selection, AC-15)
        CAST(stitching_defects     AS INT)       AS stitching_defects,
        CAST(colorfastness_defects AS INT)       AS colorfastness_defects,
        CAST(sizing_defects        AS INT)       AS sizing_defects,
        CAST(material_defects      AS INT)       AS material_defects,
        CAST(print_defects         AS INT)       AS print_defects,

        -- Dominant defect type (pre-computed for RCA routing)
        CASE
            WHEN stitching_defects     = GREATEST(stitching_defects, colorfastness_defects,
                                                   sizing_defects, material_defects, print_defects)
                 THEN 'stitching'
            WHEN colorfastness_defects = GREATEST(stitching_defects, colorfastness_defects,
                                                   sizing_defects, material_defects, print_defects)
                 THEN 'colorfastness'
            WHEN sizing_defects        = GREATEST(stitching_defects, colorfastness_defects,
                                                   sizing_defects, material_defects, print_defects)
                 THEN 'sizing'
            WHEN material_defects      = GREATEST(stitching_defects, colorfastness_defects,
                                                   sizing_defects, material_defects, print_defects)
                 THEN 'material'
            ELSE 'print'
        END                                      AS dominant_defect_type,

        TRIM(inspector_id)                       AS inspector_id,
        TRIM(qc_result)                          AS qc_result,    -- 'PASS' | 'FAIL' | 'CONDITIONAL'
        CAST(ingested_at AS TIMESTAMP)           AS ingested_at
    FROM deduped
    WHERE rn = 1
      AND units_inspected > 0   -- business rule: no zero-unit inspections
)

SELECT * FROM cleaned
