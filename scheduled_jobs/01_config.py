# Databricks notebook source
from utilities.classes.config import Config
from utilities.classes.sessionconnector import get_spark
from utilities.classes.customlogger import get_logger

def main(spark=None):
    spark = spark or get_spark()
    logger = get_logger(__name__)
    cfg = Config(spark)
    cfg.load()
    cfg.apply_spark_confs()
    logger.info("Config loaded")
    return cfg

if __name__ == "__main__":
    main()