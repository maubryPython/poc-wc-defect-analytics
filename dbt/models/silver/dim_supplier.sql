-- silver.dim_supplier
-- Source: supplier_manifest.csv (ingested by A-01)
-- Grain: one row per supplier
-- Removes duplicates and casts types. is_active derived from status field.

{{ config(materialized='table', schema='silver') }}

WITH raw AS (
    SELECT *
    FROM bronze.supplier_manifest
    WHERE _ingest_status = 'VALID'         -- quarantined rows excluded by A-01
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY supplier_id
               ORDER BY ingested_at DESC   -- keep most recent ingest if re-uploaded
           ) AS rn
    FROM raw
),

cleaned AS (
    SELECT
        UPPER(TRIM(supplier_id))             AS supplier_id,
        TRIM(supplier_name)                  AS supplier_name,
        TRIM(country)                        AS country,
        TRIM(region)                         AS region,
        TRIM(tier)                           AS tier,             -- 'TIER_1' | 'TIER_2' | 'TIER_3'
        CAST(on_time_delivery_rate AS DOUBLE) AS on_time_delivery_rate,
        CAST(avg_defect_rate       AS DOUBLE) AS avg_defect_rate,
        CAST(years_partnered       AS INT)    AS years_partnered,
        CASE
            WHEN LOWER(TRIM(status)) = 'active' THEN TRUE
            ELSE FALSE
        END                                  AS is_active,
        CAST(ingested_at AS TIMESTAMP)       AS ingested_at
    FROM deduped
    WHERE rn = 1
)

SELECT * FROM cleaned
