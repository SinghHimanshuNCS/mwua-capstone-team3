import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold_mwua.fct_consumption_billing",
    comment="Zone x billing_period consumption and billing summary. suspect_reading_count surfaces negative-consumption rows for investigation.",
    table_properties={"quality": "gold_mwua"},
)
def gold_fct_consumption_billing():
    df = dlt.read("silver_mwua.billing_transaction")
    return df.groupBy("service_zone", "billing_period").agg(
        F.sum("consumption_value_m3").alias("total_consumption_m3"),
        F.sum("amount_billed").alias("total_billed"),
        F.sum((F.col("payment_status_clean") == "OVERDUE").cast("int")).alias("overdue_accounts"),
        F.count("*").alias("total_accounts"),
        F.sum(F.col("is_suspect_reading").cast("int")).alias("suspect_reading_count"),
    )