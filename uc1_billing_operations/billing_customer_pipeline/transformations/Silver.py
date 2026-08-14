import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# STAGE 1 — account-level validity (internal view, not a catalog table)
# ---------------------------------------------------------------------------
@dlt.view(comment="Internal: bronze billing rows flagged for missing account_id.")
def _account_validated():
    df = dlt.read("bronze_mwua.billing_raw")
    return df.withColumn(
        "_dq_reason",
        F.when(F.col("account_id").isNull(), F.lit("missing_account_id")).otherwise(F.lit(None)),
    )


@dlt.table(
    name="silver_mwua.dim_customer_account",
    comment="Customer account dimension — non-PII attributes only. Grain: one row per account_id.",
    table_properties={"quality": "silver_mwua"},
)
def silver_dim_customer_account():
    df = dlt.read("_account_validated").filter("_dq_reason IS NULL")
    w = Window.partitionBy("account_id").orderBy(F.col("_ingest_ts").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select("account_id", "meter_id", "service_zone")
    )


# @dlt.table(
#     name="silver_mwua.pii_customer",
#     comment="Isolated PII table — customer_name, address, contact_number. pii_-prefixed so all sensitive tables are grep-able for audit. contact_number is dynamically masked (see governance_setup.sql).",
#     table_properties={"quality": "silver_mwua"},
# )
# def silver_pii_customer():
#     df = dlt.read("_account_validated").filter("_dq_reason IS NULL")
#     w = Window.partitionBy("account_id").orderBy(F.col("_ingest_ts").desc())
#     return (
#         df.withColumn("_rn", F.row_number().over(w))
#         .filter("_rn = 1")
#         .select("account_id", "customer_name", "address", "contact_number")
#     )


# Change Starts here......
# ---------------------------------------------------------------------------
# PII — must be a genuine Delta table (not a materialized view), so Unity
# Catalog's dynamic column mask evaluates per querying user, not once at
# pipeline-refresh time. Built via apply_changes (native "latest row wins"
# dedup) which produces a true streaming target table.
# ---------------------------------------------------------------------------
@dlt.view(comment="Internal: streaming PII rows with a valid account_id.")
def _pii_stream():
    return (
        dlt.read_stream("bronze_mwua.billing_raw")
        .filter("account_id IS NOT NULL")
        .select("account_id", "customer_name", "address", "contact_number", "_ingest_ts")
    )


dlt.create_streaming_table(
    name="silver_mwua.pii_customer",
    comment="Isolated PII table — customer_name, address, contact_number. Built via apply_changes so it is a genuine Delta table (required for Unity Catalog dynamic column masking, which cannot attach to a materialized view). pii_-prefixed so all sensitive tables are grep-able for audit.",
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target="silver_mwua.pii_customer",
    source="_pii_stream",
    keys=["account_id"],
    sequence_by="_ingest_ts",
)
### Changed until here to convert the View into Streaming table for PII_CUSTOMER table as we need to apply masking on this table....


@dlt.table(
    name="silver_mwua.reject_customer_raw",
    comment="Quarantined bronze_mwua.billing_raw rows with a missing account_id — unusable by any downstream table. PII columns intentionally excluded from this table; use _source_file to trace back to the original file for investigation.",
    table_properties={"quality": "silver_mwua"},
)
def silver_reject_customer_raw():
    df = dlt.read("_account_validated").filter("_dq_reason IS NOT NULL")
    return (
        df.select("meter_id", "service_zone", "billing_period", "_dq_reason", "_source_file", "_ingest_ts")
        .withColumn("_rejected_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# STAGE 2 — transaction-level validity (internal view)
# ---------------------------------------------------------------------------
@dlt.view(comment="Internal: valid-account rows flagged for missing billing_period or duplicate account+period.")
def _billing_validated():
    df = dlt.read("_account_validated").filter("_dq_reason IS NULL")
    w = Window.partitionBy("account_id", "billing_period").orderBy(F.col("_ingest_ts").desc())
    df = df.withColumn("_rn", F.row_number().over(w))
    return df.withColumn(
        "_dq_reason",
        F.when(F.col("billing_period").isNull(), F.lit("missing_billing_period"))
        .when(F.col("_rn") > 1, F.lit("duplicate_account_period"))
        .otherwise(F.lit(None)),
    )


@dlt.table(
    name="silver_mwua.billing_transaction",
    comment="Billing fact table. Grain: account_id x billing_period. payment_status standardized, consumption unit-converted to m3. Negative consumption flagged (is_suspect_reading), not dropped — it's valid data worth investigating, unlike the rows in reject_billing_transaction.",
    table_properties={"quality": "silver_mwua"},
)
@dlt.expect("no_negative_consumption", "consumption_value_m3 >= 0")
def silver_billing_transaction():
    df = dlt.read("_billing_validated").filter("_dq_reason IS NULL")
    return df.select(
        "account_id",
        "billing_period",
        "service_zone",
        "amount_billed",
        F.when(F.upper(F.col("payment_status")).isin("PAID"), F.lit("PAID"))
        .when(F.upper(F.col("payment_status")).isin("PEND", "PENDING"), F.lit("PENDING"))
        .otherwise(F.lit("OVERDUE"))
        .alias("payment_status_clean"),
        F.when(F.col("consumption_unit") == "L", F.col("consumption_value").cast("double") / 1000)
        .otherwise(F.col("consumption_value").cast("double"))
        .alias("consumption_value_m3"),
    ).withColumn("is_suspect_reading", F.col("consumption_value_m3") < 0)


@dlt.table(
    name="silver_mwua.reject_billing_transaction",
    comment="Quarantined rows with a valid account_id but a missing billing_period or a duplicate (account_id, billing_period). Evidence: 17 exact duplicates found during profiling — captured here for review, not silently dropped.",
    table_properties={"quality": "silver_mwua"},
)
def silver_reject_billing_transaction():
    df = dlt.read("_billing_validated").filter("_dq_reason IS NOT NULL")
    return (
        df.select("account_id", "billing_period", "service_zone", "amount_billed", "_dq_reason", "_source_file", "_ingest_ts")
        .withColumn("_rejected_at", F.current_timestamp())
    )