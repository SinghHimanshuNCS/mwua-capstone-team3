"""
Gold layer — Finance & Contractor Operations

Answers: "What's total spend by zone and by project, and which
contractors are doing the most work where?"
"""

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold_mwua.fct_spend",
    comment="Zone x project ERP spend, combined with zone x contractor work-order spend. Full outer join so a zone with only ERP spend or only contractor spend still appears.",
    table_properties={"quality": "gold"},
)
def gold_fct_spend():
    header = dlt.read("silver_mwua.invoice_header")
    lines = dlt.read("silver_mwua.invoice_line_item")
    erp = (
        header.join(lines, "invoice_id")
        .groupBy("site_zone", "project_code")
        .agg(F.sum("line_total").alias("erp_spend"))
        .withColumnRenamed("site_zone", "zone")
    )

    wo = dlt.read("silver_mwua.contractor_workorder")
    contractor = wo.groupBy("zone", "contractor_id").agg(
        F.sum("cost_sgd").alias("contractor_spend"),
        F.count("*").alias("workorder_count"),
    )

    return erp.join(contractor, "zone", "full_outer")