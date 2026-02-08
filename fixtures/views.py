def create_dim_job_view(spark):
    spark.createDataFrame([], "job_hk string, job_title string").createOrReplaceTempView("dim_job_delta")