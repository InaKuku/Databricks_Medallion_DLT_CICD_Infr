from pyspark.sql import SparkSession
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp



@dp.table(
    name="jira_tickets_bronze",
    comment="Raw Jira tickets ingested via Auto Loader"
)
def jira_tickets_bronze():

    spark = SparkSession.builder.getOrCreate()
    
    LANDING_ZONE_PATH = spark.conf.get("landing_zone_path")
    CHECKPOINT_PATH = spark.conf.get("checkpoint_path")

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation",CHECKPOINT_PATH) 
        .option("cloudFiles.inferColumnTypes", "true")
        .load(LANDING_ZONE_PATH)
        .select("*", "_metadata.file_path", current_timestamp().alias("ingestion_time"))
    )
