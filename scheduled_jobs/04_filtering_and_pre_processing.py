# Databricks notebook source
from utilities.classes.customlogger import get_logger
from pyspark.sql.functions import col

def main(spark, cfg, source_dict_df):
    logger = get_logger(__name__)
    # Standardize dict keys to simple table names to avoid ambiguity
    renamed = {}
    for fqname, df in source_dict_df.items():
        simple = fqname.split(".")[-1]
        # Enforce case-insensitive columns for consistent processing
        for c in df.columns:
            df = df.withColumnRenamed(c, c.lower())
        renamed[simple] = df
        logger.info(f"Preprocessed: {fqname} -> {simple}")
    return renamed