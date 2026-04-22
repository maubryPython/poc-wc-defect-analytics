-- silver.dim_batch
-- Source: manufacturing_batches.csv (ingested by A-01)
-- Grain: one row per batch_id
-- NOTE: is_defective column is stripped by the Ingestion Agent (A-01)
--       and never appears in Silver or Gold. It exists ONLY in the raw
--       synthetic CSV as a ground-truth label for POC validation.

{{ config(materialized='table', schema='silver') }}

WITH raw AS (
    SELECT *
    FROM bronze.manufacturing_batches
    WHERE _ingest_status = 'VALID'
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY batch_id
               ORDER BY ingested_at DESC
           ) AS rn
    FROM raw
),

cleaned AS (
    SELECT
        UPPER(TRIM(batch_id))                 AS batch_id,
        UPPER(TRIM(supplier_id))              AS supplier_id,
        UPPER(TRIM(line_id))                  AS line_id,         -- 'L1'–'L4'
        TRIM(sku)                             AS sku,
        CAST(production_date AS DATE)         AS production_date,
        CAST(unit_count      AS INT)          AS unit_count,
        CAST(shift_id        AS INT)          AS shift_id,
        TRIM(lot_id)                          AS lot_id,
        TRIM(material_grade)                  AS material_grade,  -- 'A'|'B'|'C'
        TRIM(machine_id)                      AS machine_id,
        CAST(ambient_temp_c  AS DOUBLE)       AS ambient_temp_c,
        CAST(humidity_pct    AS DOUBLE)       AS humidity_pct,
        CAST(ingested_at     AS TIMESTAMP)    AS ingested_at
    FROM deduped
    WHERE rn = 1
)

SELECT * FROM cleaned
