# HR DW ETL on Azure Databricks (Delta)

This repo implements a cost-optimized, batch ETL pipeline using Azure Databricks, following PEP8/KISS and skewless join best practices. It builds dimensions and facts defined in a central data model CSV stored in ADLS (abfss).

Key features
- Central YAML config in ADLS driving env, table DDLs, filters, and settings
- Source ingestion via spark.table with pushdown filters
- Skewless/broadcast joins and AQE enabled
- Dimensions/facts creation driven by the provided data model
- Delta Lake MERGE for dims (SCD1), append/dedup for facts
- Minimal, fast unit tests

Prereqs
- Azure Databricks workspace with cluster (Photon recommended)
- ABFS access configured (service principal or passthrough)
- App Insights optional (set connection string or disable)

Config (YAML) expected keys (example)
- env_name: dev
- settings:
    shuffle_partitions: 800
    broadcast_threshold: 104857600
    salt_buckets: 8
- data_model_csv_path: abfss://<container>@<account>.dfs.core.windows.net/config/hr_data_model.csv
- target_catalog: hive_metastore
- target_database: databricks_dw
- target_tables:
    - name: dim_employee_delta
      ddl: CREATE TABLE IF NOT EXISTS hive_metastore.databricks_dw.dim_employee_delta (employee_hk STRING, employee_id INT, first_name STRING, last_name STRING, gender STRING, birth_date DATE, email STRING) USING DELTA
    - name: dim_department_delta
      ddl: CREATE TABLE IF NOT EXISTS hive_metastore.databricks_dw.dim_department_delta (department_hk STRING, department_name STRING) USING DELTA
    - name: dim_job_delta
      ddl: CREATE TABLE IF NOT EXISTS hive_metastore.databricks_dw.dim_job_delta (job_hk STRING, job_title STRING) USING DELTA
    - name: dim_location_delta
      ddl: CREATE TABLE IF NOT EXISTS hive_metastore.databricks_dw.dim_location_delta (location_hk STRING, city STRING) USING DELTA
    - name: dim_date_delta
      ddl: CREATE TABLE IF NOT EXISTS hive_metastore.databricks_dw.dim_date_delta (date_key INT, date_value DATE) USING DELTA
    - name: fact_employee_work_delta
      ddl: CREATE TABLE IF NOT EXISTS hive_metastore.databricks_dw.fact_employee_work_delta (work_date DATE, employee_hk STRING, job_hk STRING, hours_worked DECIMAL(10,2), leave_hours DECIMAL(10,2)) USING DELTA
    - name: fact_payroll_delta
      ddl: CREATE TABLE IF NOT EXISTS hive_metastore.databricks_dw.fact_payroll_delta (payroll_fact_hk STRING, payroll_id INT, gross_pay DECIMAL(12,2), tax_amount DECIMAL(12,2), net_pay DECIMAL(12,2)) USING DELTA
- source_filters:
    databricks_hr.attendance_delta: work_date >= date_sub(current_date(), 35)
    databricks_hr.leave_records_delta: work_date >= date_sub(current_date(), 60)

Run locally (job task or notebook):
- Set env widgets or Spark conf:
  - app.config.yaml_uri = abfss://<container>@<account>.dfs.core.windows.net/config/etl_config.yml
- Execute scheduled_jobs/00_pipeline.py

Testing
- %pip install pytest delta-spark
- Run tests: python -m pytest tests -q