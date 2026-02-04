from pyspark.sql.functions import *

def daily_customer_spend(df):
    return (
        df.groupBy("customer_id", "txn_date")
          .agg(
              sum("amount").alias("daily_spend"),
              count("*").alias("txn_count")
          )
    )
