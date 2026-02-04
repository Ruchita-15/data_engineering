from pyspark.sql import SparkSession
from pyspark.sql.types import *

def read_transactions(spark, path):
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("account_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("country", StringType(), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("status", StringType(), True)
    ])

    return spark.read.csv(path, header=True, schema=schema)
