# Databricks notebook source
from utilities.classes.customlogger import get_logger
from utilities.functions.custom_functions import safe_read_table

def main(spark, cfg):
    logger = get_logger(__name__)
    source_dict_df = {}

    # Define all sources encountered in the CSV model
    sources = [
        "databricks_hr.employees_delta",
        "databricks_hr.employee_personal_delta",
        "databricks_hr.employee_contact_delta",
        "databricks_hr.departments_delta",
        "databricks_hr.jobs_delta",
        "databricks_hr.office_locations_delta",
        "databricks_hr.attendance_delta",
        "databricks_hr.leave_records_delta",
        "databricks_hr.job_history_delta",
        "databricks_hr.payroll_transactions_delta",
    ]

    for src in sources:
        filter_sql = cfg.source_filters.get(src)
        df = safe_read_table(spark, src, filter_sql)
        source_dict_df[src] = df
        logger.info(f"Loaded source: {src} rows={df.count()}")

    return source_dict_df