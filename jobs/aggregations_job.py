import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from src.utils import create_spark_session, get_logger, write_df, get_path
from src.aggregations import daily_customer_spend


def main():
    spark = create_spark_session(
        os.path.join(project_root, "configs", "spark_config.yaml")
    )
    logger = get_logger("aggregations-job")

    silver_df = spark.read.parquet(
        get_path(os.path.join(project_root, "data", "processed"),
                 "silver", "transactions_enriched")
    )

    gold_df = daily_customer_spend(silver_df).repartition("txn_date")

    write_df(
        gold_df,
        get_path(os.path.join(project_root, "data", "processed"),
                 "gold", "daily_customer_spend"),
        partition_col="txn_date"
    )

    logger.info("Aggregations job completed")


if __name__ == "__main__":
    main()
