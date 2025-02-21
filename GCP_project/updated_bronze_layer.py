import pyspark
from pyspark.sql import SparkSession
import json
from google.cloud import storage
import os
from pyspark.sql.functions import current_timestamp, lit


def get_latest_csv_file(input_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(input_path.split('/')[2])
    prefix = '/'.join(input_path.split('/')[3:])
    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        return None
    latest_blob = max(blobs, key=lambda b: b.updated)
    return f"gs://{bucket.name}/{latest_blob.name}"


def process_and_save_latest_csv(spark, input_path, output_path):
    latest_file_path = get_latest_csv_file(input_path)

    if latest_file_path:
        df = spark.read.option("header", "true").csv(latest_file_path)
        df = df.withColumn("ingest_timestamp", current_timestamp())
        df = df.withColumn("source", lit("generated"))
        df.printSchema()
        df.show(5)
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        print(f"Processed and saved: {latest_file_path} to {output_path}")
    else:
        print(f"No CSV files found in {input_path}, skipping...")


def main():
    spark = SparkSession.builder.appName("Dataproc Pipeline").getOrCreate()

    bucket_name = "raw-data-generated-by-script"
    run_date = "20250221"
    base_path = "gs://raw-data-generated-by-script/data"

    input_folders = {
        "customers": f"{base_path}/raw_customers/{run_date}",
        "orders": f"{base_path}/raw_orders/{run_date}",
        "order_items": f"{base_path}/raw_order_items/{run_date}",
        "products": f"{base_path}/raw_products/{run_date}"
    }

    output_folders = {
        "customers": f"{base_path}/refined_customers",
        "orders": f"{base_path}/refined_orders",
        "order_items": f"{base_path}/refined_order_items",
        "products": f"{base_path}/refined_products"
    }

    for key in input_folders:
        process_and_save_latest_csv(spark, input_folders[key], output_folders[key])

    spark.stop()
    print("Dataproc Pipeline Execution Completed")


if __name__ == "__main__":
    main()
