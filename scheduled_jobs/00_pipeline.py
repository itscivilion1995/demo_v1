# Databricks notebook source
# Orchestration entry. Runs steps in order.
# Supports both notebook workflow or module-style execution.

from utilities.classes.sessionconnector import get_spark
from utilities.classes.config import Config
from utilities.classes.customlogger import get_logger

from scheduled_jobs import (
    _01_config as job_config,
    _02_create_target_tables as create_targets,
    _03_source_data_load as load_sources,
    _04_filtering_and_pre_processing as preprocess,
    _05_master_dataset_creation as build_master,
    _06_dim_transformations as dim_tx,
    _07_fact_transformations as fact_tx,
    _09_data_load as load_targets,
)

def main():
    spark = get_spark()
    logger = get_logger(__name__)
    logger.info("Pipeline started")

    cfg = job_config.main(spark)
    create_targets.main(spark, cfg)

    src = load_sources.main(spark, cfg)
    src = preprocess.main(spark, cfg, src)

    master = build_master.main(spark, cfg, src)
    dims = dim_tx.main(spark, cfg, master, src)
    facts = fact_tx.main(spark, cfg, master, src, dims)

    load_targets.main(spark, cfg, dims, facts)
    logger.info("Pipeline completed")

if __name__ == "__main__":
    main()