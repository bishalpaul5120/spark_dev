import os
import sys
import subprocess
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("snowjob")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# Get Snowflake password securely from AWS Secrets Manager
# ============================================================

snowpassword = subprocess.getoutput(
    "aws secretsmanager get-secret-value "
    "--secret-id snowpass "
    "--query SecretString "
    "--output text"
).strip()

if not snowpassword:
    raise Exception("Snowflake password could not be loaded from Secrets Manager")

# ============================================================
# Read from Snowflake
# ============================================================

sdf = (
    spark.read.format("snowflake")
    .option("sfUrl", "https://IBPOCCB-KP90206.snowflakecomputing.com")
    .option("sfAccount", "KP90206")
    .option("sfUser", "bishalpaul5120")
    .option("sfPassword", snowpassword)
    .option("sfDatabase", "CUSTOMER")
    .option("sfSchema", "PUBLIC")
    .option("sfRole", "ACCOUNTADMIN")
    .option("sfWarehouse", "COMPUTE_WH")
    .option("dbTable", "CUSTOMERDATA")
    .load()
)

agedf = sdf.filter("AGE > 30")

agedf.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3://clean-curated-data/snowOutput/")
