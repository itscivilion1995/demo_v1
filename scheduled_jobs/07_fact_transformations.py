# Databricks notebook source
from utilities.classes.customlogger import get_logger
from utilities.functions.custom_functions import (
    coalesce_zero, scd_lookup_latest_asof, safe_numeric_cast
)
from pyspark.sql import functions as F

def main(spark, cfg, master, src, dims):
    logger = get_logger(__name__)
    facts = {}

    # Fact: Employee Work (attendance + leave + lookups)
    att = src.get("attendance_delta")
    if att is not None:
        # employee_hk via hash lookup from dim_employee
        dim_emp = dims.get("dim_employee_delta")
        emp_lkp = dim_emp.select("employee_id", "employee_hk")

        # Join attendance with hashed employee
        f = (
            att.alias("a")
            .join(F.broadcast(emp_lkp).alias("de"), F.col("a.employee_id") == F.col("de.employee_id"), "left")
            .withColumn("work_date", F.to_date("a.work_date"))
            .withColumn("hours_worked", F.col("hours_worked").cast("decimal(10,2)"))
        )

        # Join leave hours if present on (employee_id, work_date)
        leave = src.get("leave_records_delta")
        if leave is not None and set(["employee_id", "leave_hours"]).issubset(set(leave.columns)):
            leave = leave.withColumn("work_date", F.to_date("work_date"))
            f = (
                f.join(
                    leave.select("employee_id", "work_date", "leave_hours")
                         .withColumn("leave_hours", F.col("leave_hours").cast("decimal(10,2)")),
                    on=["employee_id", "work_date"],
                    how="left",
                )
            )
        else:
            f = f.withColumn("leave_hours", F.lit(None).cast("decimal(10,2)"))

        f = f.withColumn("leave_hours", F.coalesce(F.col("leave_hours"), F.lit(0.0).cast("decimal(10,2)")))

        # job_hk via SCD_LOOKUP (job_history as-of work_date)
        job_hist = src.get("job_history_delta")
        dim_job = dims.get("dim_job_delta")
        if job_hist is not None and dim_job is not None:
            # Expect job_history has employee_id, job_id, start_date, end_date (optional)
            job_hist = job_hist.withColumn("start_date", F.to_date("start_date"))
            if "end_date" in job_hist.columns:
                job_hist = job_hist.withColumn("end_date", F.to_date("end_date"))
            else:
                job_hist = job_hist.withColumn("end_date", F.lit(None).cast("date"))

            f = scd_lookup_latest_asof(
                fact_df=f,
                dim_df=job_hist.select("employee_id", "job_id", "start_date", "end_date"),
                fact_key_col="employee_id",
                dim_key_col="employee_id",
                asof_col="work_date",
                dim_start_col="start_date",
                dim_end_col="end_date",
                select_cols=["job_id"],
                salt_buckets=cfg.salt_buckets,
            )

            f = (
                f.join(
                    F.broadcast(dim_job),
                    on=F.sha2(F.col("job_id").cast("string"), 256) == F.col("job_hk"),
                    how="left",
                )
            )
        else:
            f = f.withColumn("job_hk", F.lit(None).cast("string"))

        fact_work = f.select(
            "work_date",
            "employee_hk",
            "job_hk",
            "hours_worked",
            "leave_hours",
        ).dropDuplicates(["employee_hk", "work_date"])

        facts["fact_employee_work_delta"] = fact_work
        logger.info(f"Prepared fact_employee_work_delta rows={fact_work.count()}")

    # Fact: Payroll
    pay = src.get("payroll_transactions_delta")
    if pay is not None:
        fact_pay = (
            pay
            .withColumn("payroll_fact_hk", F.sha2(F.col("payroll_id").cast("string"), 256))
            .withColumn("gross_pay", F.col("gross_pay").cast("decimal(12,2)"))
            .withColumn("tax_amount", F.col("tax_amount").cast("decimal(12,2)"))
            .withColumn("net_pay", (F.col("gross_pay") - F.col("tax_amount")).cast("decimal(12,2)"))
            .select("payroll_fact_hk", "payroll_id", "gross_pay", "tax_amount", "net_pay")
            .dropDuplicates(["payroll_fact_hk"])
        )
        facts["fact_payroll_delta"] = fact_pay
        logger.info(f"Prepared fact_payroll_delta rows={fact_pay.count()}")

    return facts