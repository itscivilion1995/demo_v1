# Databricks notebook source
from utilities.classes.customlogger import get_logger
from pyspark.sql import functions as F

def main(spark, cfg, master, src):
    logger = get_logger(__name__)
    dims = {}

    emp = master["employee_master"]
    dims["dim_employee_delta"] = emp

    # Department
    dep = src.get("departments_delta")
    if dep is not None:
        dims["dim_department_delta"] = dep.select(
            F.sha2(F.col("department_id").cast("string"), 256).alias("department_hk"),
            F.trim(F.col("department_name")).alias("department_name"),
        ).dropDuplicates(["department_hk"])

    # Job
    jobs = src.get("jobs_delta")
    if jobs is not None:
        dims["dim_job_delta"] = jobs.select(
            F.sha2(F.col("job_id").cast("string"), 256).alias("job_hk"),
            F.trim(F.col("job_title")).alias("job_title"),
        ).dropDuplicates(["job_hk"])

    # Location
    loc = src.get("office_locations_delta")
    if loc is not None:
        dims["dim_location_delta"] = loc.select(
            F.sha2(F.col("location_id").cast("string"), 256).alias("location_hk"),
            F.initcap(F.col("city")).alias("city"),
        ).dropDuplicates(["location_hk"])

    # Date
    if "date_master" in master:
        dims["dim_date_delta"] = master["date_master"].select("date_key", "date_value")

    for k, v in dims.items():
        logger.info(f"Prepared dim: {k} rows={v.count()}")

    return dims