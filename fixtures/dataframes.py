from pyspark.sql import Row

def employees_df(spark):
    return spark.createDataFrame([
        Row(employee_id=1),
        Row(employee_id=2),
    ])