import pyspark
from pyspark.sql import SparkSession
import os
from datetime import datetime
from pyspark.sql.functions import col
from google.cloud import storage

def get_latest_csv_file(input_path):
    print(f"📂 Fetching latest CSV from: {input_path}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(input_path.split('/')[2])
    prefix = '/'.join(input_path.split('/')[3:])
    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        print(f"⚠️ No files found in {input_path}")
        return None
    latest_blob = max(blobs, key=lambda b: b.updated)
    print(f"✅ Latest file: gs://{bucket.name}/{latest_blob.name}")
    return f"gs://{bucket.name}/{latest_blob.name}"

# Initialize Spark Session
print("🚀 Starting Spark Session")
spark = SparkSession.builder.appName("GoldLayerProcessing").getOrCreate()

# Hardcoded Paths
silver_output_path = "gs://raw-data-generated-by-script/data/silver_"
gold_output_path = "gs://raw-data-generated-by-script/data/gold_"

# Input directories
input_folders = {
    "customers": f"{silver_output_path}customers",
    "orders": f"{silver_output_path}orders",
    "order_items": f"{silver_output_path}order_items",
    "products": f"{silver_output_path}products"
}

# Output directories
output_folders = {
    "customers": f"{gold_output_path}customers",
    "orders": f"{gold_output_path}orders",
    "order_items": f"{gold_output_path}order_items",
    "products": f"{gold_output_path}products"
}

# Get latest files
print("🔍 Retrieving latest files from Silver Layer")
latest_files = {key: get_latest_csv_file(input_folders[key]) for key in input_folders}

if not all(latest_files.values()):
    print("❌ Missing required files in Silver Layer. Ensure all data exist.")
    spark.stop()
    exit(1)

# Read CSV files
print("📥 Reading input data")
customers_df = spark.read.option("header", True).csv(latest_files["customers"])
order_items_df = spark.read.option("header", True).csv(latest_files["order_items"])
orders_df = spark.read.option("header", True).csv(latest_files["orders"])
products_df = spark.read.option("header", True).csv(latest_files["products"])

# Debugging: Print schema of all dataframes
print("🔬 Schema of order_items_df:")
order_items_df.printSchema()
print("🔬 Schema of orders_df:")
orders_df.printSchema()
print("🔬 Schema of products_df:")
products_df.printSchema()

# Check if 'customer_id' exists in orders_df
if "customer_id" in orders_df.columns:
    print("🔗 Joining order_items with orders to get 'customer_id'")
    order_items_df = order_items_df.join(orders_df.select("order_id", "customer_id"), "order_id", "left")
else:
    raise Exception("❌ Column 'customer_id' not found in orders data. Ensure orders dataset is correct.")

# Rename `customer_id` in `order_items_df` to avoid ambiguity in join
if "customer_id" in order_items_df.columns:
    print("🔄 Renaming 'customer_id' to 'os_customer_id' in order_items_df")
    order_items_df = order_items_df.withColumnRenamed('customer_id', 'os_customer_id')
else:
    raise Exception("❌ Column 'customer_id' not found after join. Check orders dataset.")

# Check if 'customer_id' exists in customers_df
print("🔍 Checking 'customer_id' column in customers_df")
if "customer_id" not in customers_df.columns:
    raise Exception("❌ Column 'customer_id' not found in customers data.")

# Perform LEFT JOIN on `customer_id`
print("🔗 Joining order_items with customers on 'customer_id'")
final_df = order_items_df.join(customers_df, order_items_df.os_customer_id == customers_df.customer_id, 'left')

# Filter out `CANCELLED` orders if order_status column exists
if "order_status" in final_df.columns:
    print("🗑️ Filtering out CANCELLED orders")
    final_df = final_df.filter(
        (final_df.os_customer_id.isNotNull()) &
        (final_df.order_status != "CANCELLED")
    )
else:
    print("⚠️ Column 'order_status' not found. Skipping order cancellation filtering.")

# Drop old `customer_id` column and rename `os_customer_id` back to `customer_id`
print("🔄 Renaming 'os_customer_id' back to 'customer_id'")
if "customer_id" in final_df.columns:
    final_df = final_df.drop('customer_id')
final_df = final_df.withColumnRenamed('os_customer_id', 'customer_id')

# Write transformed data to Gold Layer Output
for key in output_folders:
    current_date = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{output_folders[key]}/gold_final_{key}_data_{timestamp}.csv"
    print(f"💾 Writing final transformed data for {key} to: {output_file}")
    final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_file)
    print(f"✅ Final Gold Layer Data successfully written to: {output_file}")

# Stop Spark Session
print("🛑 Stopping Spark Session")
spark.stop()