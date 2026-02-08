from utilities.functions.custom_functions import hash_sha256_str, yyyymmdd
from pyspark.sql import functions as F

def test_hash_sha256_str(spark):
    df = spark.createDataFrame([(1, "A")], "id int, s string").select(hash_sha256_str("id", "s").alias("hk"))
    assert df.collect()[0]["hk"] is not None

def test_yyyymmdd(spark):
    df = spark.createDataFrame([("2024-02-10",)], "d string").select(yyyymmdd(F.to_date("d")).alias("k"))
    assert df.collect()[0]["k"] == 20240210