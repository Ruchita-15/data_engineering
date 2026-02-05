import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from src.utils import create_spark_session, get_logger, write_df, get_path
from src.ingestion import read_transactions


def main():
    spark = create_spark_session(
        os.path.join(project_root, "configs", "spark_config.yaml")
    )
    logger = get_logger("ingestion-job")

    txn_df = read_transactions(
        spark,
        os.path.join(project_root, "data", "raw", "transactions.csv")
    )

    bronze_path = get_path(
        os.path.join(project_root, "data", "processed"),
        "bronze",
        "transactions"
    )

    write_df(txn_df, bronze_path)
    logger.info("Bronze ingestion completed")


if __name__ == "__main__":
    main()
