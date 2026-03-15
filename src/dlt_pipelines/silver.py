import sys, os

from pyspark import pipelines as dp
from pyspark.sql.functions import col

sys.path.append(os.path.abspath(".."))

from common.transformations import clean_text_column
from common.quality_rules import SILVER_EXPECTATIONS


@dp.table(name="jira_tickets_silver")
@dp.expect_all(SILVER_EXPECTATIONS)
def jira_tickets_silver():
    df = (
        dp.read_stream("jira_tickets_bronze")
        .select(
            col("key").alias("ticket_key"),
            col("fields.summary").alias("summary"),
            col("fields.status.name").alias("status"),
            col("fields.created").cast("timestamp").alias("created_at"),
            col("fields.description").alias("raw_description")
        )
    )

    return clean_text_column(df)