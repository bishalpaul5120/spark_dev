import os
import sys

from pyspark.sql.functions import *
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = r"D:\hadoop"
os.environ['PATH'] = os.environ['PATH'] + r";D:\hadoop\bin"
os.environ['JAVA_HOME'] = r"C:\Users\HP\.jdks\corretto-1.8.0_462"

spark = SparkSession.builder.appName("employee").getOrCreate()
sc = spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")
#======================================================================

empdf = spark.read.format("json").option("multiline","true").load("s3://spark-jobs1/src/")
empdf.printSchema()


flatarr = empdf.withColumn("projects", explode(col("projects"))).withColumn("skills", explode(col("skills")))
flatarr.printSchema()


flatstructs = flatarr.select("address.city",
                             "address.pincode",
                             "address.street",
                             "department",
                             "hire_date",
                             "id",
                             "manager.manager_id",
                             "manager.manager_name",
                             "name",
                             "projects.duration_months",
                             "projects.project_id",
                             "projects.project_name",
                             "projects.tech_stack",
                             "salary",
                             "skills")
flatstructs.printSchema()

finaldf = flatstructs.withColumn("tech_stack", explode(col("tech_stack")))
finaldf.show()

salaryfil = finaldf.filter("salary > 120000")

datedf = finaldf.withColumn("hire_date", to_date("hire_date"))
yeardf = datedf.withColumn("Year", year("hire_date"))
countemp = yeardf.groupBy("Year").agg(count("*").alias("Total_employee")).orderBy("Year")

salaryfil.write.format("parquet").mode("append").save("s3://spark-jobs1/dest/s3Output/")
countemp.write.format("parquet").mode("append").save("s3://spark-jobs1/dest/s3Output/")



