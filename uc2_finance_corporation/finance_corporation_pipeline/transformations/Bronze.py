"""
Bronze layer — Finance & Contractor Operations

Two source shapes, landed separately, raw fidelity, no cleanup:
  1. ERP JSON (paginated envelope: meta + data array of invoices, nested
     vendor + line_items) — landed as one row per file (per page).
  2. Contractor CSVs (3 different schemas) — landed with native column
     names, contractor_id injected at ingestion since none of the files
     self-identify their contractor.
"""

import dlt
from pyspark.sql import functions as F

ERP_PATH = "/Volumes/mwua_capstone_team3/bronze_mwua/land_erp"
CONTRACTOR_PATHS = {
    "A": "/Volumes/mwua_capstone_team3/bronze_mwua/land_contractor_a",
    "B": "/Volumes/mwua_capstone_team3/bronze_mwua/land_contractor_b",
    "C": "/Volumes/mwua_capstone_team3/bronze_mwua/land_contractor_c",
}


@dlt.table(
    name="bronze_mwua.erp_invoice_raw",
    comment="Raw ERP paginated JSON extract, landed as-is (one row per page/file). vendor and line_items kept nested — flattening/exploding is a Silver decision made with full context.",
    table_properties={"quality": "bronze"},
)
def bronze_erp_invoice_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(ERP_PATH)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


def _make_contractor_bronze(contractor_id, path):
    @dlt.table(
        name=f"bronze_mwua.contractor_{contractor_id.lower()}_raw",
        comment=f"Raw work-order CSV extract from Contractor {contractor_id}, native column names preserved. contractor_id injected here since the file itself doesn't self-identify.",
        table_properties={"quality": "bronze"},
    )
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(path)
            .withColumn("contractor_id", F.lit(contractor_id))
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )
    return _bronze


for _cid, _path in CONTRACTOR_PATHS.items():
    _make_contractor_bronze(_cid, _path)