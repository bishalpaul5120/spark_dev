import os
import sys
import subprocess
import json

from pyspark.sql.functions import *
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = r"D:\hadoop"
os.environ['PATH'] = os.environ['PATH'] + r";D:\hadoop\bin"
os.environ['JAVA_HOME'] = r"C:\Users\HP\.jdks\corretto-1.8.0_462"

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("masterJob")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# =====================================================================

s3data = spark.read.parquet("s3://spark-jobs1/dest/s3Output/")
snowdata = spark.read.parquet(
    "s3://spark-jobs1/dest/snowOutput/part-00000-c88834f0-35bc-4760-bf3c-364b6c46a16d-c000.snappy.parquet"
)

s3data.createOrReplaceTempView("sdata")
snowdata.createOrReplaceTempView("snow")

result = spark.sql("""
    SELECT DISTINCT
        sn.name,
        sn.city,
        s.department,
        s.project_name,
        s.salary
    FROM snow sn
    INNER JOIN sdata s
    ON sn.city = s.city
""")

result.show(truncate=False)



