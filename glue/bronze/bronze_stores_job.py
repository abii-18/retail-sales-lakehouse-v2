import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit
)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_ROOT_PATH", "BRONZE_ROOT_PATH", "BATCH_DATE"])

sc = SparkContext()

glue_context = GlueContext(sc)

spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

batch_date = args["BATCH_DATE"]

raw_stores_path = f"{args['RAW_ROOT_PATH']}/stores/batch_date={batch_date}"
bronze_stores_path = f"{args['BRONZE_ROOT_PATH']}/stores/batch_date={batch_date}"

print(f"Batch Date : {batch_date}")
print(f"Raw Path : {raw_stores_path}")
print(f"Output Path : {bronze_stores_path}")

stores_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw_stores_path)
)

stores_df = (
    stores_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("batch_date", lit(batch_date))
    .withColumn("source_system", lit("postgres"))
    .withColumn("source_file", input_file_name())
)

print("Schema")
stores_df.printSchema()

print("Sample Records")
stores_df.show(5, truncate=False)

print(f"Rows Read : {stores_df.count()}")

(
    stores_df.write
    .mode("overwrite")
    .parquet(bronze_stores_path)
)

print("Bronze Stores written successfully.")

job.commit()
