-- silver.fact_sales_returns
-- Source: sales_returns.csv (ingested by A-01)
-- Grain: one row per return_id
-- Used by Scenario Agent (A-04) for return rate baseline and cost modeling.

{{ config(materialized='table', schema='silver') }}

WITH raw AS (
    SELECT *
    FROM bronze.sales_returns
    WHERE _ingest_status = 'VALID'
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY return_id
               ORDER BY ingested_at DESC
           ) AS rn
    FROM raw
),

cleaned AS (
    SELECT
        TRIM(return_id)                          AS return_id,
        UPPER(TRIM(batch_id))                    AS batch_id,
        UPPER(TRIM(supplier_id))                 AS supplier_id,
        TRIM(sku)                                AS sku,
        CAST(sale_date    AS DATE)               AS sale_date,
        CAST(return_date  AS DATE)               AS return_date,

        -- Days between sale and return (used in timeline modeling)
        DATEDIFF(
            CAST(return_date AS DATE),
            CAST(sale_date   AS DATE)
        )                                        AS days_to_return,

        CAST(units_sold     AS INT)              AS units_sold,
        CAST(units_returned AS INT)              AS units_returned,
        ROUND(
            CAST(units_returned AS DOUBLE) / NULLIF(units_sold, 0),
            6
        )                                        AS return_rate,

        TRIM(return_reason)                      AS return_reason,
        CAST(unit_price_usd AS DOUBLE)           AS unit_price_usd,
        ROUND(
            CAST(units_returned AS DOUBLE) * CAST(unit_price_usd AS DOUBLE),
            2
        )                                        AS gross_return_cost_usd,

        TRIM(channel)                            AS channel,      -- 'retail' | 'ecommerce' | 'wholesale'
        CAST(ingested_at AS TIMESTAMP)           AS ingested_at
    FROM deduped
    WHERE rn = 1
      AND units_sold > 0
)

SELECT * FROM cleaned
