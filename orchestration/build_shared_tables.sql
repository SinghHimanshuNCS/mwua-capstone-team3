USE CATALOG mwua_capstone_team3;

-- ----------------------------------------------------------------------------
-- dim_zone: derived (unioned), not hardcoded.
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW silver_mwua.dim_zone AS
SELECT DISTINCT service_zone AS zone_name FROM silver_mwua.billing_transaction
UNION
SELECT DISTINCT site_zone AS zone_name FROM silver_mwua.invoice_header
UNION
SELECT DISTINCT zone AS zone_name FROM silver_mwua.contractor_workorder
UNION
SELECT DISTINCT zone AS zone_name FROM silver_mwua.sensor_reading;

COMMENT ON TABLE silver_mwua.dim_zone IS
  'Shared zone reference, derived (unioned) from all 3 usecases'' Silver tables, not hardcoded.';

-- ----------------------------------------------------------------------------
-- rpt_zone_360: the literal answer to "can MWUA see consumption, spend, and
-- network health together, by zone, in one place." Grain = zone only, since
-- the 3 source facts have incompatible time-grains (monthly billing_period,
-- daily reading_date, no time-grain on spend) — forcing a shared time-grain
-- would misrepresent the data. LEFT JOIN from dim_zone so every known zone
-- appears even if one usecase has no data for it yet.
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_mwua.rpt_zone_360 AS
WITH billing_agg AS (
  SELECT service_zone AS zone,
         SUM(total_consumption_m3) AS total_consumption_m3,
         SUM(total_billed)         AS total_billed,
         SUM(overdue_accounts)     AS overdue_accounts,
         SUM(suspect_reading_count) AS billing_suspect_count
  FROM gold_mwua.fct_consumption_billing
  GROUP BY service_zone
),
spend_agg AS (
  SELECT zone,
         SUM(erp_spend)        AS total_erp_spend,
         SUM(contractor_spend) AS total_contractor_spend,
         SUM(workorder_count)  AS total_workorder_count
  FROM gold_mwua.fct_spend
  GROUP BY zone
),
network_agg AS (
  SELECT zone,
         AVG(reliability_pct) AS avg_reliability_pct,
         SUM(reading_count)   AS total_readings,
         SUM(suspect_count)   AS network_suspect_count
  FROM gold_mwua.fct_network_health
  GROUP BY zone
)
SELECT
  dz.zone_name                    AS zone,
  b.total_consumption_m3,
  b.total_billed,
  b.overdue_accounts,
  b.billing_suspect_count,
  s.total_erp_spend,
  s.total_contractor_spend,
  s.total_workorder_count,
  n.avg_reliability_pct,
  n.total_readings,
  n.network_suspect_count
FROM silver_mwua.dim_zone dz
LEFT JOIN billing_agg b ON dz.zone_name = b.zone
LEFT JOIN spend_agg  s ON dz.zone_name = s.zone
LEFT JOIN network_agg n ON dz.zone_name = n.zone;

COMMENT ON TABLE gold_mwua.rpt_zone_360 IS
  'Cross-cutting zone-level view combining consumption/billing, spend, and network health. Grain: zone only (the 3 source facts have incompatible time-grains). rpt_-prefixed since it combines multiple usecases, unlike fct_ tables which stay within one usecase.';