from src.transformations import enrich_transactions

def test_enrich_transactions_adds_columns(spark):
    txn_data = [
        ("TXN1", "ACC1", "CUST1", 15000.0, "2025-01-01 10:00:00", "UAE"),
    ]
    txn_cols = [
        "transaction_id",
        "account_id",
        "customer_id",
        "amount",
        "transaction_timestamp",
        "country",
    ]

    cust_data = [
        ("CUST1", "Retail", "UAE"),
    ]
    cust_cols = ["customer_id", "segment", "country"]

    acc_data = [
        ("ACC1", "CUST1", "Savings"),
    ]
    acc_cols = ["account_id", "customer_id", "account_type"]

    txn_df = spark.createDataFrame(txn_data, txn_cols)
    cust_df = spark.createDataFrame(cust_data, cust_cols)
    acc_df = spark.createDataFrame(acc_data, acc_cols)

    enriched_df = enrich_transactions(txn_df, cust_df, acc_df)

    columns = enriched_df.columns

    assert "txn_date" in columns
    assert "is_high_value_txn" in columns
    assert "transaction_country" in columns
    assert "customer_country" in columns

    row = enriched_df.collect()[0]
    assert row.is_high_value_txn == 1
