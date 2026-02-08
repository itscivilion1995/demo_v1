# Databricks notebook source
from utilities.classes.customlogger import get_logger
from utilities.functions.custom_functions import merge_upsert_delta, append_delta

def main(spark, cfg, dims, facts):
    logger = get_logger(__name__)

    # Dimensions: SCD1 upsert on HK
    dim_keys = {
        "dim_employee_delta": ["employee_hk"],
        "dim_department_delta": ["department_hk"],
        "dim_job_delta": ["job_hk"],
        "dim_location_delta": ["location_hk"],
        "dim_date_delta": ["date_key"],
    }
    for name, df in dims.items():
        full_name = f"{cfg.target_catalog}.{cfg.target_database}.{name}"
        keys = dim_keys.get(name)
        if keys:
            merge_upsert_delta(spark, df, full_name, keys)
        else:
            append_delta(spark, df, full_name)
        logger.info(f"Loaded dimension: {full_name}")

    # Facts: append with dedup before write
    fact_targets = list(facts.items())
    for name, df in fact_targets:
        full_name = f"{cfg.target_catalog}.{cfg.target_database}.{name}"
        append_delta(spark, df, full_name)
        logger.info(f"Loaded fact: {full_name}")