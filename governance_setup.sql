-- ============================================================================
-- Billing & Customer — Governance Setup (run ONCE after the pipeline has
-- successfully created its Silver tables)
-- ============================================================================

USE CATALOG mwua_capstone_team3;
CREATE SCHEMA IF NOT EXISTS governance_mwua;

-- ----------------------------------------------------------------------------
-- Masking function
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION governance_mwua.mask_contact(contact STRING)
RETURN CASE
  WHEN is_account_group_member('MWUA PII Data Reader') THEN contact
  WHEN contact IS NULL THEN NULL
  ELSE concat('XXXX', right(contact, 4))
END;

ALTER TABLE silver_mwua.pii_customer
  ALTER COLUMN contact_number SET MASK governance_mwua.mask_contact;

-- ----------------------------------------------------------------------------
-- RBAC grants
-- ----------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON SCHEMA mwua_capstone_team3.bronze_mwua TO `MWUA Data Engineers`;
GRANT ALL PRIVILEGES ON SCHEMA mwua_capstone_team3.silver_mwua TO `MWUA Data Engineers`;
GRANT ALL PRIVILEGES ON SCHEMA mwua_capstone_team3.gold_mwua   TO `MWUA Data Engineers`;

GRANT USE CATALOG ON CATALOG mwua_capstone_team3 TO `MWUA Data Analyst`;
GRANT USE SCHEMA ON SCHEMA mwua_capstone_team3.gold_mwua TO `MWUA Data Analyst`;
GRANT SELECT ON SCHEMA mwua_capstone_team3.gold_mwua TO `MWUA Data Analyst`;

GRANT SELECT ON TABLE mwua_capstone_team3.silver_mwua.pii_customer TO `MWUA Data Analyst`;

-- Reject/quarantine tables: engineers only, not analysts.
GRANT SELECT ON TABLE mwua_capstone_team3.silver_mwua.reject_customer_raw TO `MWUA Data Engineers`;
GRANT SELECT ON TABLE mwua_capstone_team3.silver_mwua.reject_billing_transaction TO `MWUA Data Engineers`;

-- ----------------------------------------------------------------------------
-- Catalog comments
-- ----------------------------------------------------------------------------
COMMENT ON TABLE silver_mwua.dim_customer_account IS
  'Customer account dimension. One row per account_id. Non-PII.';
COMMENT ON TABLE silver_mwua.pii_customer IS
  'Isolated PII: customer_name, address, contact_number. contact_number is dynamically masked — unmasked only for mwua_pii_readers group members.';
COMMENT ON TABLE silver_mwua.billing_transaction IS
  'Billing fact table, grain account_id x billing_period. payment_status standardized to PAID/PENDING/OVERDUE.';
COMMENT ON TABLE silver_mwua.reject_customer_raw IS
  'Quarantined billing_raw rows with missing account_id. PII columns excluded by design.';
COMMENT ON TABLE silver_mwua.reject_billing_transaction IS
  'Quarantined billing_transaction candidates: missing billing_period or duplicate account_id+billing_period.';
COMMENT ON TABLE gold_mwua.fct_consumption_billing IS
  'Zone x billing_period consumption and billing summary. suspect_reading_count flags negative consumption.';

-- For testing
GRANT SELECT ON TABLE mwua_capstone_team3.silver_mwua.pii_customer TO `MWUA PII Data Reader`;

-- For Usecase 2

GRANT SELECT ON TABLE mwua_capstone_team3.silver_mwua.reject_invoice TO `MWUA Data Engineers`;
GRANT SELECT ON TABLE mwua_capstone_team3.silver_mwua.reject_contractor_workorder TO `MWUA Data Engineers`;

COMMENT ON TABLE silver_mwua.invoice_header IS
  'Invoice header, one row per invoice_id. currency_is_inferred flags rows where currency was defaulted.';
COMMENT ON TABLE silver_mwua.dim_vendor IS
  'Vendor reference table, safe direct extract from ERP.';
COMMENT ON TABLE silver_mwua.contractor_workorder IS
  'Reconciled work orders across 3 contractors, config-driven mapping.';
COMMENT ON TABLE gold_mwua.fct_spend IS
  'Zone x project x contractor spend summary, combining ERP invoices and contractor work orders.';





