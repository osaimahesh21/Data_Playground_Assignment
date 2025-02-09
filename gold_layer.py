import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ✅ Initialize Spark Session
spark = SparkSession.builder \
    .appName("GoldLayerProcessing") \
    .getOrCreate()

# ✅ Load JSON configuration
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "config.json")

with open(config_path, "r") as file:
    config = json.load(file)

# ✅ Get paths from config
gold_layer_input_base = config["gold_layer_input"]["base_path"]
gold_layer_output_base = config["gold_layer_output"]
date_folder = config["gold_layer_input"]["date_folder"]

# ✅ Ensure output directory exists
os.makedirs(gold_layer_output_base, exist_ok=True)

# ✅ Function to get the latest file from a directory
def get_latest_file(directory):
    if not os.path.exists(directory):
        print(f"⚠️ Directory not found: {directory}")
        return None
    csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]
    if not csv_files:
        return None
    latest_file = max(csv_files, key=lambda f: os.path.getmtime(os.path.join(directory, f)))
    return os.path.join(directory, latest_file)

# ✅ Construct paths for customers and order items
customers_dir = os.path.join(gold_layer_input_base, "customers", date_folder)
order_items_dir = os.path.join(gold_layer_input_base, "order_items", date_folder)

# ✅ Get latest files
customers_file = get_latest_file(customers_dir)
order_items_file = get_latest_file(order_items_dir)

if not customers_file or not order_items_file:
    print("❌ Missing required files. Ensure both customers and order items data exist.")
    spark.stop()
    exit(1)

# ✅ Read CSV files
customers_df = spark.read.option("header", True).csv(customers_file)
order_items_df = spark.read.option("header", True).csv(order_items_file)
print("🔍 Order Items Columns Before Renaming:", order_items_df.columns)

# ✅ Select required columns from customers
customers_df = customers_df.select('customer_id', 'city', 'state')

# ✅ Rename `customer_id` in `order_items_df` to avoid ambiguity in join
order_items_df = order_items_df.withColumnRenamed('customer_id', 'os_customer_id')

# ✅ Perform LEFT JOIN on `customer_id`
final_df = order_items_df.join(customers_df, order_items_df.os_customer_id == customers_df.customer_id, 'left')

# ✅ Filter out `CANCELLED` orders
final_df = final_df.filter(
    (final_df.os_customer_id.isNotNull()) &
    (final_df.order_status != "CANCELLED")
)

# ✅ Drop old `customer_id` column and rename `os_customer_id` back to `customer_id`
final_df = final_df.drop('customer_id').withColumnRenamed('os_customer_id', 'customer_id')

# ✅ Handle missing columns dynamically
existing_columns = set(final_df.columns)
expected_columns = [
    'customer_id', 'product_id', 'order_id', 'order_item_id', 'quantity', 'unit_price',
    'orders_ingest_timestamp', 'orders_source_system', 'order_items_ingest_timestamp', 'order_items_source_system',
    'order_date', 'total_amount', 'order_status', 'city', 'state'
]

# Select only existing columns to avoid AnalysisException
valid_columns = [col for col in expected_columns if col in existing_columns]
final_df = final_df.select(*valid_columns)

# ✅ Generate output path with timestamp
current_date = datetime.now().strftime("%Y%m%d")
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = os.path.join(gold_layer_output_base, "final_orders_data", current_date)
os.makedirs(output_dir, exist_ok=True)

output_file = os.path.join(output_dir, f"gold_final_orders_data_{timestamp}.csv")

# ✅ Write transformed data to Gold Layer Output
final_df.write.mode("overwrite").option("header", True).csv(output_file)
print(f"✅ Final Gold Layer Data written to: {output_file}")

# ✅ Stop Spark Session
spark.stop()
