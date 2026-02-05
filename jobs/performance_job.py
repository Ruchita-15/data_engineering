import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from src.utils import create_spark_session, get_logger, get_path


def main():
    spark = create_spark_session(
        os.path.join(project_root, "configs", "spark_config.yaml")
    )
    logger = get_logger("performance-job")

    df = spark.read.parquet(
        get_path(os.path.join(project_root, "data", "processed"),
                 "silver", "transactions_enriched")
    )

    df.repartition("txn_date").groupBy("txn_date").sum("amount").explain(True)

    logger.info("Performance analysis completed")


if __name__ == "__main__":
    main()
