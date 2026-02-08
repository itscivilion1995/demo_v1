# Databricks notebook source
from utilities.classes.customlogger import get_logger
from utilities.functions.custom_functions import (
    trim_upper, trim_lower, to_initcap, to_date_cast,
    hash_sha256_str, skewless_join, broadcast_left, broadcast_right
)
from pyspark.sql import functions as F

def main(spark, cfg, src):
    """
    Build curated masters. Focused on employee_master used by dims/facts.
    """
    logger = get_logger(__name__)

    emp = src.get("employees_delta")
    per = src.get("employee_personal_delta")
    con = src.get("employee_contact_delta")

    if emp is None:
        raise ValueError("employees_delta source is missing")

    # Optional tables may be empty
    per = per or spark.createDataFrame([], schema="employee_id INT, first_name STRING, last_name STRING, gender STRING, birth_date DATE")
    con = con or spark.createDataFrame([], schema="employee_id INT, email STRING")

    # Normalize fields per model (TRIM/UPPER/LOWER/DATE_CAST)
    per = (
        per
        .withColumn("first_name", F.trim(F.col("first_name")))
        .withColumn("last_name", F.trim(F.col("last_name")))
        .withColumn("gender", F.upper(F.col("gender")))
        .withColumn("birth_date", F.to_date(F.col("birth_date")))
    )
    con = con.withColumn("email", F.lower(F.col("email")))

    # Join with skew handling: employees is typically large; personal/contact small; broadcast them
    joined = (
        emp.alias("e")
        .join(F.broadcast(per).alias("p"), F.col("e.employee_id") == F.col("p.employee_id"), "left")
        .join(F.broadcast(con).alias("c"), F.col("e.employee_id") == F.col("c.employee_id"), "left")
    )

    # Employee HK per model: SHA256(employee_id)
    employee_master = joined.select(
        F.sha2(F.col("e.employee_id").cast("string"), 256).alias("employee_hk"),
        F.col("e.employee_id").alias("employee_id"),
        F.col("p.first_name").alias("first_name"),
        F.col("p.last_name").alias("last_name"),
        F.col("p.gender").alias("gender"),
        F.col("p.birth_date").alias("birth_date"),
        F.col("c.email").alias("email"),
    ).dropDuplicates(["employee_id"])

    logger.info(f"employee_master rows={employee_master.count()}")

    # Date master from attendance (for dim_date)
    att = src.get("attendance_delta")
    if att is not None and "work_date" in att.columns:
        date_master = att.select(
            F.to_date("work_date").alias("date_value"),
            F.date_format("work_date", "yyyyMMdd").cast("int").alias("date_key"),
        ).dropDuplicates(["date_key"])
    else:
        date_master = spark.createDataFrame([], schema="date_value DATE, date_key INT")

    masters = {
        "employee_master": employee_master,
        "date_master": date_master
    }
    return masters