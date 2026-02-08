# Databricks notebook source
from utilities.classes.customlogger import get_logger
from utilities.functions.custom_functions import ensure_database, execute_ddl

def main(spark, cfg):
    logger = get_logger(__name__)
    ensure_database(spark, cfg.target_catalog, cfg.target_database)

    if not cfg.target_tables:
        logger.warning("No target table DDLs defined in config")
        return

    for tdef in cfg.target_tables:
        ddl = tdef.get("ddl")
        if ddl:
            execute_ddl(spark, ddl)
            logger.info(f"Ensured target table: {tdef.get('name')}")