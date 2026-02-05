from src.validations import validate_transactions

def test_validate_transactions_separates_valid_and_invalid(spark):
    data = [
        ("TXN1", "ACC1", "CUST1", 100.0, "2025-01-01 10:00:00"),
        ("TXN2", "ACC2", "CUST2", -50.0, "2025-01-01 11:00:00"),  # invalid
        ("TXN3", "ACC3", "CUST3", 200.0, None),                 # invalid
    ]

    columns = [
        "transaction_id",
        "account_id",
        "customer_id",
        "amount",
        "transaction_timestamp",
    ]

    df = spark.createDataFrame(data, columns)

    valid_df, invalid_df = validate_transactions(df)

    assert valid_df.count() == 1
    assert invalid_df.count() == 2
