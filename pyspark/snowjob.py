import json
import boto3
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("snowjob").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read secret
client = boto3.client("secretsmanager", region_name="ap-south-1")
secret = json.loads(client.get_secret_value(SecretId="snowpass")["SecretString"])
snowpassword = secret["password"]

sfOptions = {
    "sfURL": "https://ibpoccb-kp90206.snowflakecomputing.com",
    "sfUser": "bishalpaul5120",
    "sfPassword": snowpassword,
    "sfDatabase": "CUSTOMER",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
}

sdf = (
    spark.read
    .format("snowflake")
    .options(**sfOptions)
    .option("dbtable", "CUSTOMERDATA")
    .load()
)

agedf = sdf.filter("AGE > 30")
agedf.write.mode("overwrite").parquet("s3://clean-curated-data/snowOutput/")
