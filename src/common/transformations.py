from pyspark.sql import DataFrame
from pyspark.sql.functions import trim, lower, col, regexp_replace


def clean_text_column(df: DataFrame) -> DataFrame:
    """
    Cleans the 'raw_description' column in a Spark DataFrame by:
      - Lowercasing and trimming whitespace
      - Removing punctuation
      - Normalizing whitespace to single spaces
    Args:
        df (DataFrame): Input DataFrame with a 'raw_description' column.
    Returns:
        DataFrame: DataFrame with the cleaned 'raw_description' column.
    """
    cleaned = (
        df
        .withColumn("raw_description", trim(lower(col("raw_description"))))
        .withColumn("raw_description", regexp_replace(col("raw_description"), r"[\p{Punct}]", ""))
        .withColumn("raw_description", regexp_replace(col("raw_description"), r"\s+", " "))
    )
    return cleaned 
