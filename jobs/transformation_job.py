import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from src.utils import create_spark_session, get_logger, write_df, get_path
from src.transformations import enrich_transactions


def main():
    spark = create_spark_session(
        os.path.join(project_root, "configs", "spark_config.yaml")
    )
    logger = get_logger("transformations-job")

    txn_df = spark.read.parquet(
        get_path(os.path.join(project_root, "data", "processed"),
                 "silver", "transactions_clean")
    )

    customers_df = spark.read.csv(
        os.path.join(project_root, "data", "raw", "customers.csv"),
        header=True, inferSchema=True
    )

    accounts_df = spark.read.csv(
        os.path.join(project_root, "data", "raw", "accounts.csv"),
        header=True, inferSchema=True
    )

    enriched_df = enrich_transactions(txn_df, customers_df, accounts_df)

    write_df(
        enriched_df,
        get_path(os.path.join(project_root, "data", "processed"),
                 "silver", "transactions_enriched")
    )

    logger.info("Transformations job completed")


if __name__ == "__main__":
    main()
