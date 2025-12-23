import json
import subprocess
from pyspark.sql import SparkSession

# ============================================================
# Spark session
# ============================================================
spark = SparkSession.builder.appName("snowjob").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# Read secret from AWS Secrets Manager (CLI – EMR SAFE)
# ============================================================
secret_raw = subprocess.getoutput(
    "aws secretsmanager get-secret-value "
    "--secret-id snowpass "
    "--query SecretString "
    "--output text"
)

secret = json.loads(secret_raw)
snowpassword = secret["password"]

if not snowpassword:
    raise Exception("Snowflake password is empty")

# ============================================================
# Snowflake options (CORRECT)
# ============================================================
sfOptions = {
    "sfURL": "https://ibpoccb-kp90206.snowflakecomputing.com",
    "sfUser": "bishalpaul5120",
    "sfPassword": snowpassword,
    "sfDatabase": "CUSTOMER",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
}

# ============================================================
# Read from Snowflake
# ============================================================
sdf = (
    spark.read
    .format("snowflake")
    .options(**sfOptions)
    .option("dbtable", "CUSTOMERDATA")
    .load()
)

agedf = sdf.filter("AGE > 30")

agedf.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("s3://clean-curated-data/snowOutput/")
