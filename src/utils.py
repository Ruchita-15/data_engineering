import yaml
import logging
import os
from pyspark.sql import SparkSession


# ---------------------------------------------------
# Logger
# ---------------------------------------------------
def get_logger(app_name: str) -> logging.Logger:
    logger = logging.getLogger(app_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# ---------------------------------------------------
# Spark Session Creator
# ---------------------------------------------------
def create_spark_session(config_path: str) -> SparkSession:
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)["spark"]

    spark = (
        SparkSession.builder
        .appName(config["app"]["name"])
        .master(config["master"])
        .config("spark.executor.memory", config["executor"]["memory"])
        .config("spark.executor.cores", config["executor"]["cores"])
        .config("spark.driver.memory", config["driver"]["memory"])
        .config("spark.sql.shuffle.partitions", config["sql"]["shuffle"]["partitions"])
        .config("spark.sql.adaptive.enabled", config["sql"]["adaptive"]["enabled"])
        .config("spark.serializer", config["serializer"])
        .config("spark.network.timeout", config["network"]["timeout"])
        .getOrCreate()
    )

    return spark


# ---------------------------------------------------
# DataFrame Writer
# ---------------------------------------------------
def write_df(df, path: str, mode: str = "overwrite", partition_col: str = None):
    writer = df.write.mode(mode)

    if partition_col:
        writer = writer.partitionBy(partition_col)

    writer.parquet(path)


# ---------------------------------------------------
# Standard Path Builder
# ---------------------------------------------------
def get_path(base_path: str, layer: str, table: str) -> str:
    return os.path.join(base_path, layer, table)
