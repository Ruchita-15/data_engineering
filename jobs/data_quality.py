import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from src.utils import create_spark_session, get_logger, write_df, get_path
from src.validations import validate_transactions


def main():
    spark = create_spark_session(
        os.path.join(project_root, "configs", "spark_config.yaml")
    )
    logger = get_logger("data-quality-job")

    bronze_path = get_path(
        os.path.join(project_root, "data", "processed"),
        "bronze",
        "transactions"
    )

    bronze_df = spark.read.parquet(bronze_path)

    valid_df, invalid_df = validate_transactions(bronze_df)

    write_df(
        valid_df,
        get_path(os.path.join(project_root, "data", "processed"),
                 "silver", "transactions_clean")
    )

    write_df(
        invalid_df,
        get_path(os.path.join(project_root, "data", "processed"),
                 "silver", "transactions_invalid")
    )

    logger.info("Data quality job completed")


if __name__ == "__main__":
    main()
