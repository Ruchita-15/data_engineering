from pyspark.sql.functions import col

def validate_transactions(df):
    valid_df = df.filter(
        (col("amount") > 0) &
        (col("transaction_timestamp").isNotNull())
    )

    invalid_df = df.subtract(valid_df)
    return valid_df, invalid_df
