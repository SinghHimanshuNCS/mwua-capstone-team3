"""
Silver layer — Network Sensor Telemetry

Key design decision (from profiling): sensor_id -> location_id is stable,
but location_id -> zone is NOT (12/16 locations showed 2-5 different zone
labels across readings). So:
  - dim_sensor_location is a MATERIALIZED VIEW (batch) that derives the
    trusted zone per location as the mode of observed values — this is
    intentionally batch, not streaming, since "most frequent historically"
    is an aggregate concept, not a per-event one.
  - sensor_reading is a STREAMING TABLE, stream-static joined against
    dim_sensor_location, built via apply_changes for native dedup on
    (sensor_id, timestamp) — 161 duplicates found during profiling.
  - Implausible readings (negative pressure/flow) and zone mismatches are
    FLAGGED, not dropped — this is the literal answer to "where is the
    data too unreliable to trust."
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# STAGE 1 — completeness validation (streaming view)
# ---------------------------------------------------------------------------
@dlt.view(comment="Internal: streaming bronze readings flagged for missing keys.")
def _sensor_validated():
    df = dlt.read_stream("bronze_mwua.sensor_raw")
    return df.withColumn(
        "_dq_reason",
        F.when(F.col("sensor_id").isNull(), F.lit("missing_sensor_id"))
        .when(F.col("location_id").isNull(), F.lit("missing_location_id"))
        .when(F.col("timestamp").isNull(), F.lit("missing_timestamp"))
        .when(F.col("reading_value").isNull(), F.lit("missing_reading_value"))
        .otherwise(F.lit(None)),
    )


@dlt.table(
    name="silver_mwua.reject_sensor_reading",
    comment="Quarantined bronze.sensor_raw rows missing a key field (sensor_id, location_id, timestamp, or reading_value) — unusable for any downstream table.",
    table_properties={"quality": "silver"},
)
def silver_reject_sensor_reading():
    df = dlt.read_stream("_sensor_validated").filter("_dq_reason IS NOT NULL")
    return (
        df.select("sensor_id", "location_id", "zone", "reading_type", "_dq_reason", "_source_file", "_ingest_ts")
        .withColumn("_rejected_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# STAGE 2 — dim_sensor_location: trusted zone via mode (materialized, batch)
# ---------------------------------------------------------------------------
@dlt.table(
    name="silver_mwua.dim_sensor_location",
    comment="Trusted location -> zone mapping, derived as the mode (most frequent) observed zone per location_id. Deliberately a materialized view, not streaming — 'most frequent historically' is a batch aggregate concept. Evidence: 12/16 locations showed multiple zone labels during profiling.",
    table_properties={"quality": "silver"},
)
def silver_dim_sensor_location():
    df = dlt.read("bronze_mwua.sensor_raw").filter(
        F.col("sensor_id").isNotNull()
        & F.col("location_id").isNotNull()
        & F.col("timestamp").isNotNull()
        & F.col("reading_value").isNotNull()
    )
    w = Window.partitionBy("location_id").orderBy(F.col("count").desc())
    return (
        df.groupBy("location_id", "zone").count()
        .withColumn("_rank", F.row_number().over(w))
        .filter("_rank = 1")
        .select("location_id", F.col("zone").alias("trusted_zone"))
    )


# ---------------------------------------------------------------------------
# STAGE 3 — sensor_reading: stream-static join + implausibility flag,
# deduped via apply_changes.
# ---------------------------------------------------------------------------
@dlt.view(comment="Internal: valid readings enriched with trusted zone and quality flags, ready for dedup.")
def _sensor_enriched():
    readings = dlt.read_stream("_sensor_validated").filter("_dq_reason IS NULL")
    dim = dlt.read("silver_mwua.dim_sensor_location")
    return (
        readings.join(dim, "location_id", "left")
        .withColumn("zone_mismatch_flag", F.col("zone") != F.col("trusted_zone"))
        .withColumn(
            "is_physically_implausible",
            ((F.col("reading_type") == "pressure") & (F.col("reading_value") < 0))
            | ((F.col("reading_type") == "flow") & (F.col("reading_value") < 0)),
        )
        .select(
            "sensor_id", "location_id",
            F.col("trusted_zone").alias("zone"),
            "reading_type", "reading_value", "unit", "timestamp",
            "zone_mismatch_flag", "is_physically_implausible", "_ingest_ts",
        )
    )


dlt.create_streaming_table(
    name="silver_mwua.sensor_reading",
    comment="Sensor readings, grain: one row per (sensor_id, timestamp), deduped via apply_changes — evidence: 161 duplicate (sensor_id, timestamp) readings found during profiling. zone is the trusted value from dim_sensor_location, not the raw per-reading field. is_physically_implausible and zone_mismatch_flag are surfaced (not dropped) and feed gold.fct_network_health.reliability_pct.",
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target="silver_mwua.sensor_reading",
    source="_sensor_enriched",
    keys=["sensor_id", "timestamp"],
    sequence_by="_ingest_ts",
)
