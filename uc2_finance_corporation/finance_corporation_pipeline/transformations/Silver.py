"""
Silver layer — Finance & Contractor Operations

Two-stage validation with quarantine capture, same pattern as billing:
  - ERP: invoices missing invoice_id -> reject_invoice.
  - Contractors: rows missing work_order_id, or with an unparseable
    completion_date/cost -> reject_contractor_workorder.

Reconciliation across the 3 contractor schemas is config-driven — a
4th contractor is one dict entry, not new pipeline code.
"""

import functools
import dlt
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# ERP — explode invoices, then explode line items separately
# ---------------------------------------------------------------------------
@dlt.view(comment="Internal: exploded invoice records from the paginated ERP JSON, flagged for validation.")
def _invoice_exploded():
    df = dlt.read("bronze_mwua.erp_invoice_raw")
    exploded = df.select(
        F.explode("data").alias("inv"), "_source_file", "_ingest_ts"
    ).select(
        F.col("inv.invoice_id").alias("invoice_id"),
        F.col("inv.cost_center").alias("cost_center"),
        F.col("inv.vendor.id").alias("vendor_id"),
        F.col("inv.vendor.name").alias("vendor_name"),
        F.col("inv.site_zone").alias("site_zone"),
        F.col("inv.currency").alias("currency_raw"),
        F.col("inv.invoice_date").alias("invoice_date"),
        F.col("inv.project_code").alias("project_code"),
        F.col("inv.line_items").alias("line_items"),
        "_source_file", "_ingest_ts",
    )
    return exploded.withColumn(
        "_dq_reason",
        F.when(F.col("invoice_id").isNull(), F.lit("missing_invoice_id")).otherwise(F.lit(None)),
    )


@dlt.table(
    name="silver_mwua.invoice_header",
    comment="Invoice header, grain: one row per invoice_id. currency defaulted to SGD when blank (14 records found during profiling), currency_is_inferred flags those rows so totals stay honest.",
    table_properties={"quality": "silver"},
)
def silver_invoice_header():
    df = dlt.read("_invoice_exploded").filter("_dq_reason IS NULL")
    return df.select(
        "invoice_id", "cost_center", "vendor_id", "vendor_name", "site_zone", "invoice_date", "project_code",
        F.when((F.col("currency_raw").isNull()) | (F.col("currency_raw") == ""), F.lit("SGD"))
        .otherwise(F.col("currency_raw")).alias("currency"),
        F.when((F.col("currency_raw").isNull()) | (F.col("currency_raw") == ""), F.lit(True))
        .otherwise(F.lit(False)).alias("currency_is_inferred"),
    )


@dlt.table(
    name="silver_mwua.reject_invoice",
    comment="Quarantined ERP records with a missing invoice_id — unusable by any downstream table.",
    table_properties={"quality": "silver"},
)
def silver_reject_invoice():
    df = dlt.read("_invoice_exploded").filter("_dq_reason IS NOT NULL")
    return (
        df.select("cost_center", "vendor_id", "vendor_name", "site_zone", "invoice_date", "project_code",
                   "_dq_reason", "_source_file", "_ingest_ts")
        .withColumn("_rejected_at", F.current_timestamp())
    )


@dlt.table(
    name="silver_mwua.invoice_line_item",
    comment="Exploded line items, grain: one row per invoice_id + line_no. line_total computed once here so no downstream consumer re-derives it.",
    table_properties={"quality": "silver"},
)
def silver_invoice_line_item():
    df = dlt.read("_invoice_exploded").filter("_dq_reason IS NULL")
    return (
        df.select("invoice_id", F.explode("line_items").alias("li"))
        .select(
            "invoice_id",
            F.col("li.line_no").alias("line_no"),
            F.col("li.description").alias("description"),
            F.col("li.qty").alias("qty"),
            F.col("li.unit_cost").alias("unit_cost"),
            (F.col("li.qty") * F.col("li.unit_cost")).alias("line_total"),
        )
    )


@dlt.table(
    name="silver_mwua.dim_vendor",
    comment="Vendor reference table. Safe direct extract — profiling found zero vendor.id <-> vendor.name conflicts.",
    table_properties={"quality": "silver"},
)
def silver_dim_vendor():
    df = dlt.read("_invoice_exploded").filter("_dq_reason IS NULL")
    return df.select("vendor_id", "vendor_name").distinct()


# ---------------------------------------------------------------------------
# Contractors — config-driven reconciliation across 3 raw schemas
# ---------------------------------------------------------------------------
CONTRACTOR_CONFIG = {
    "A": {"wo_id": "work_order_id", "zone": "site_location", "desc": "work_description",
          "date": "date_completed", "date_fmt": "yyyy-MM-dd", "cost": "cost_sgd", "cost_is_text": False},
    "B": {"wo_id": "WO_Number", "zone": "Location", "desc": "Desc",
          "date": "CompletionDate", "date_fmt": "dd/MM/yyyy", "cost": "Amount", "cost_is_text": False},
    "C": {"wo_id": "id", "zone": "loc_id", "desc": "notes",
          "date": "completed_on", "date_fmt": "yyyy/MM/dd", "cost": "charge", "cost_is_text": True},
}


@dlt.view(comment="Internal: reconciled contractor work orders from all 3 raw schemas, flagged for validation.")
def _contractor_reconciled():
    frames = []
    for cid, cfg in CONTRACTOR_CONFIG.items():
        df = dlt.read(f"bronze_mwua.contractor_{cid.lower()}_raw")
        cost_col = (
            F.regexp_extract(F.col(cfg["cost"]), r"[\d.]+", 0).cast("double")
            if cfg["cost_is_text"] else F.col(cfg["cost"]).cast("double")
        )
        frames.append(df.select(
            F.col(cfg["wo_id"]).alias("work_order_id"),
            F.col(cfg["zone"]).alias("zone"),
            F.col(cfg["desc"]).alias("description"),
            F.to_date(F.col(cfg["date"]), cfg["date_fmt"]).alias("completion_date"),
            cost_col.alias("cost_sgd"),
            F.col("contractor_id"),
            "_source_file", "_ingest_ts",
        ))
    unioned = functools.reduce(lambda a, b: a.unionByName(b), frames)
    return unioned.withColumn(
        "_dq_reason",
        F.when(F.col("work_order_id").isNull(), F.lit("missing_work_order_id"))
        .when(F.col("completion_date").isNull(), F.lit("unparseable_completion_date"))
        .when(F.col("cost_sgd").isNull(), F.lit("unparseable_cost"))
        .otherwise(F.lit(None)),
    )


@dlt.table(
    name="silver_mwua.contractor_workorder",
    comment="Reconciled work orders across all 3 contractors, grain: one row per work_order_id. Built via config-driven mapping — a 4th contractor is one CONTRACTOR_CONFIG entry, zero new pipeline code.",
    table_properties={"quality": "silver"},
)
def silver_contractor_workorder():
    return dlt.read("_contractor_reconciled").filter("_dq_reason IS NULL")


@dlt.table(
    name="silver_mwua.reject_contractor_workorder",
    comment="Quarantined contractor rows: missing work_order_id, or a completion_date/cost that failed to parse under that contractor's expected format.",
    table_properties={"quality": "silver"},
)
def silver_reject_contractor_workorder():
    df = dlt.read("_contractor_reconciled").filter("_dq_reason IS NOT NULL")
    return (
        df.select("zone", "description", "contractor_id", "_dq_reason", "_source_file", "_ingest_ts")
        .withColumn("_rejected_at", F.current_timestamp())
    )