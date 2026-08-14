import dlt
from pyspark.sql import functions as F

LANDING_PATH = "/Volumes/mwua_capstone_team3/bronze_mwua/land_billing"


@dlt.table(
    name="bronze_mwua.billing_raw",
    comment="Raw billing & customer extract, landed as-is from the legacy DB2 CSV export.",
    table_properties={"quality": "bronze_mwua"},
)
def bronze_billing_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(LANDING_PATH)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )