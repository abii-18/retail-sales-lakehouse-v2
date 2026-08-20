import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from common.bronze import run_bronze_job


args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "RAW_ROOT_PATH", "BRONZE_ROOT_PATH", "BATCH_DATE"]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

run_bronze_job(
    spark=spark,
    raw_root_path=args["RAW_ROOT_PATH"],
    bronze_root_path=args["BRONZE_ROOT_PATH"],
    table_name="orders",
    batch_date=args["BATCH_DATE"]
)

job.commit()