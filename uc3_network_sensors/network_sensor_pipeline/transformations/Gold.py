"""
Gold layer — Network Sensor Telemetry

Answers: "What does network health look like by zone over time, and
where is the data too unreliable to trust?" — reliability_pct makes that
second question a literal, numeric answer, not a caveat in a footnote.
"""

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold_mwua.fct_network_health",
    comment="Zone x date x reading_type network health summary. reliability_pct = 1 - (implausible + zone-mismatched readings) / total readings for that group.",
    table_properties={"quality": "gold"},
)
def gold_fct_network_health():
    df = dlt.read("silver_mwua.sensor_reading")
    return (
        df.groupBy("zone", F.to_date("timestamp").alias("reading_date"), "reading_type")
        .agg(
            F.avg("reading_value").alias("avg_reading_value"),
            F.count("*").alias("reading_count"),
            F.sum(F.col("is_physically_implausible").cast("int")).alias("suspect_count"),
            F.sum(F.col("zone_mismatch_flag").cast("int")).alias("zone_mismatch_count"),
        )
        .withColumn(
            "reliability_pct",
            1 - (F.col("suspect_count") + F.col("zone_mismatch_count")) / F.col("reading_count"),
        )
    )