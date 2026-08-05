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

raw_payments_path = f"{args['RAW_ROOT_PATH']}/payments/batch_date={batch_date}"
bronze_payments_path = f"{args['BRONZE_ROOT_PATH']}/payments/batch_date={batch_date}"

print(f"Batch Date : {batch_date}")
print(f"Raw Path : {raw_payments_path}")
print(f"Output Path : {bronze_payments_path}")

payments_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw_payments_path)
)

payments_df = (
    payments_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("batch_date", lit(batch_date))
    .withColumn("source_system", lit("postgres"))
    .withColumn("source_file", input_file_name())
)

print("Schema")
payments_df.printSchema()

print("Sample Records")
payments_df.show(5, truncate=False)

print(f"Rows Read : {payments_df.count()}")

(
    payments_df.write
    .mode("overwrite")
    .parquet(bronze_payments_path)
)

print("Bronze Payments written successfully.")

job.commit()
