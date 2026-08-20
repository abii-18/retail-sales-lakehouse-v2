from pyspark.sql.functions import current_timestamp, input_file_name, lit


def run_bronze_job(spark, raw_root_path, bronze_root_path, table_name, batch_date):
    raw_path = f"{raw_root_path}/{table_name}/batch_date={batch_date}"
    bronze_path = f"{bronze_root_path}/{table_name}/batch_date={batch_date}"

    print(f"Batch Date: {batch_date}")
    print(f"Raw Path: {raw_path}")
    print(f"Output Path: {bronze_path}")

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)

    df = (
        df.withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("batch_date", lit(batch_date))
        .withColumn("source_system", lit("postgres"))
        .withColumn("source_file", input_file_name())
    )

    print("Schema:")
    df.printSchema()

    print("Sample records:")
    df.show(5, truncate=False)

    print(f"Rows read: {df.count()}")

    df.write.mode("overwrite").parquet(bronze_path)

    print(f"Bronze {table_name} written successfully.")
