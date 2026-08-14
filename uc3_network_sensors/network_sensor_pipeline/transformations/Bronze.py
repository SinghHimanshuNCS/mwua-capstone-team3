"""
Bronze layer — Network Sensor Telemetry

Streaming ingestion, triggered mode (not continuous) — readings arrive at
5-min intervals inside hourly files, which is frequent batch, not a
sub-minute latency requirement. Per-reading zone field landed as-is
(not corrected) — the inconsistency is itself a governance finding to
surface later, not quietly patch here.
"""

import dlt
from pyspark.sql import functions as F

LANDING_PATH = "/Volumes/mwua_capstone_team3/bronze_mwua/land_sensors"


@dlt.table(
    name="bronze_mwua.sensor_raw",
    comment="Raw sensor telemetry, one row per reading, landed as-is including the per-reading zone field (found unreliable during profiling — see silver_mwua.dim_sensor_location).",
    table_properties={"quality": "bronze"},
)
def bronze_sensor_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(LANDING_PATH)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )