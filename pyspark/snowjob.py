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

spark = (
    SparkSession.builder
    .appName("snowjob")
    .getOrCreate()
)


sc = spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")

#=======================================================================
secret = subprocess.getoutput("aws secretsmanager get-secret-value --secret-id snowpass --query SecretString --output text")
snowpassword = json.loads(secret)["password"]

sdf = (
    spark.read.format("snowflake")
    .option("sfUrl","https://IBPOCCB-KP90206.snowflakecomputing.com")
    .option("sfAccount","KP90206")
    .option("sfUser","bishalpaul5120")
    .option("sfPassword",snowpassword)
    .option("sfDatabase","CUSTOMER")
    .option("sfSchema","PUBLIC")
    .option("sfRole","ACCOUNTADMIN")
    .option("sfWarehouse","COMPUTE_WH")
    .option("dbTable","CUSTOMERDATA")
    .load()
)

sdf.show()


agedf = sdf.filter("AGE > 30")
agedf.show(truncate=False)
agedf.write.format("parquet").mode("overwrite").save("s3://clean-curated-data/snowOutput/")




