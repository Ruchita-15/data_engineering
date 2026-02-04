from pyspark.sql.functions import col, to_date, when

# ---------------------------------------------------
# Enrich Transactions with Customer & Account Data
# ---------------------------------------------------
def enrich_transactions(txn_df, customers_df, accounts_df):
    """
    Enrich transactions with customer and account details
    """

    # ---- STEP 1: Rename conflicting columns BEFORE join ----
    txn_df = txn_df.withColumnRenamed(
        "country", "transaction_country"
    )

    customers_df = customers_df.withColumnRenamed(
        "country", "customer_country"
    )

    # ---- STEP 2: Join with customers ----
    txn_cust_df = txn_df.join(
        customers_df,
        on="customer_id",
        how="left"
    )

    # ---- STEP 3: Join with accounts ----
    enriched_df = txn_cust_df.join(
        accounts_df,
        on=["account_id", "customer_id"],
        how="left"
    )

    # ---- STEP 4: Derived columns ----
    enriched_df = (
        enriched_df
        .withColumn("txn_date", to_date(col("transaction_timestamp")))
        .withColumn(
            "is_high_value_txn",
            when(col("amount") > 10000, 1).otherwise(0)
        )
    )

    return enriched_df
