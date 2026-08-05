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

raw_orders_path = f"{args['RAW_ROOT_PATH']}/orders/batch_date={batch_date}"
bronze_orders_path = f"{args['BRONZE_ROOT_PATH']}/orders/batch_date={batch_date}"

print(f"Batch Date : {batch_date}")
print(f"Raw Path : {raw_orders_path}")
print(f"Output Path : {bronze_orders_path}")

orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw_orders_path)
)

orders_df = (
    orders_df
    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )
    .withColumn(
        "batch_date",
        lit(batch_date)
    )
    .withColumn(
        "source_system",
        lit("postgres")
    )
    .withColumn(
        "source_file",
        input_file_name()
    )
)

print("Schema")
orders_df.printSchema()

print("Sample Records")
orders_df.show(5, truncate=False)

print(f"Rows Read : {orders_df.count()}")

(
    orders_df.write
    .mode("overwrite")
    .parquet(bronze_orders_path)
)

print("Bronze Orders written successfully.")

job.commit()
