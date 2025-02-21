import pyspark
from pyspark.sql import SparkSession
import os
import datetime
from pyspark.sql.functions import col
from google.cloud import storage
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def get_latest_csv_file(input_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(input_path.split('/')[2])
    prefix = '/'.join(input_path.split('/')[3:])
    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        return None
    latest_blob = max(blobs, key=lambda b: b.updated)
    return f"gs://{bucket.name}/{latest_blob.name}"


def clean_dataframe(df, entity_name):
    if df is not None:
        if "source" in df.columns:
            df = df.drop("source")
        df = df.withColumnRenamed("ingest_timestamp", f"{entity_name}_ingest_timestamp")
        df = df.withColumnRenamed("source_system", f"{entity_name}_source_system")
        df = df.dropDuplicates()
    return df


def process_and_save_latest_csv(spark, input_path, output_path, entity_name):
    latest_file_path = get_latest_csv_file(input_path)

    if latest_file_path:
        schema_mapping = {
            "customers": StructType([
                StructField("customer_id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("address", StringType(), True)
            ]),
            "orders": StructType([
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("order_date", StringType(), True),
                StructField("total_amount", DoubleType(), True)
            ]),
            "order_items": StructType([
                StructField("order_item_id", StringType(), True),
                StructField("order_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("price", DoubleType(), True)
            ]),
            "products": StructType([
                StructField("product_id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("price", DoubleType(), True)
            ])
        }

        schema = schema_mapping.get(entity_name)
        df = spark.read.option("header", "true").schema(schema).csv(latest_file_path)

        df = clean_dataframe(df, entity_name)
        if entity_name == "orders":
            df = df.filter(col("total_amount") >= 0)
        elif entity_name == "order_items":
            df = df.filter(col("quantity") > 0)

        temp_output_path = f"{output_path}_temp"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_output_path)

        storage_client = storage.Client()
        bucket = storage_client.bucket(output_path.split('/')[2])
        prefix = '/'.join(output_path.split('/')[3:])
        blobs = list(bucket.list_blobs(prefix=f"{prefix}_temp/"))

        renamed = False
        for blob in blobs:
            if blob.name.endswith(".csv"):
                new_blob_name = f"{prefix}/silver_{entity_name}.csv"
                bucket.rename_blob(blob, new_blob_name)
                print(f"Renamed {blob.name} to {new_blob_name}")
                renamed = True
                break

        if renamed:
            for blob in blobs:
                try:
                    blob.delete()
                except Exception as e:
                    print(f"Skipping deletion for {blob.name}: {e}")

        print(f"Processed and saved: {latest_file_path} to {output_path}/silver_{entity_name}.csv")
    else:
        print(f"No CSV files found in {input_path}, skipping...")


def main():
    spark = SparkSession.builder.appName("SilverLayerTransformation").getOrCreate()

    base_path = "gs://raw-data-generated-by-script/data"
    bronze_output_path = "gs://raw-data-generated-by-script/data/refined_"
    silver_output_path = "gs://raw-data-generated-by-script/data/silver_"

    input_folders = {
        "customers": f"{bronze_output_path}customers",
        "orders": f"{bronze_output_path}orders",
        "order_items": f"{bronze_output_path}order_items",
        "products": f"{bronze_output_path}products"
    }

    output_folders = {
        "customers": f"{silver_output_path}customers",
        "orders": f"{silver_output_path}orders",
        "order_items": f"{silver_output_path}order_items",
        "products": f"{silver_output_path}products"
    }

    for key in input_folders:
        process_and_save_latest_csv(spark, input_folders[key], output_folders[key], key)

    spark.stop()
    print("Silver Layer Processing Completed")


if __name__ == "__main__":
    main()
