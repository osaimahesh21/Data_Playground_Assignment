import os
import json
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower


def load_config():
    """Load JSON config file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get script directory
    config_file = os.path.join(script_dir, "config.json")   # Use absolute path
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file not found: {config_file}")

    with open(config_file, 'r') as f:
        return json.load(f)

def get_latest_csv_file(directory):
    """Return the latest modified CSV file in the given directory."""
    if not os.path.exists(directory):
        return None  # Directory does not exist

    csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]

    if not csv_files:
        return None  # No CSV files found

    # Get the latest file based on modification time
    latest_file = max(csv_files, key=lambda f: os.path.getmtime(os.path.join(directory, f)))
    return os.path.join(directory, latest_file)


def process_data(spark, config):
    """Read the latest CSV from each data type folder, transform, and write to Silver Layer."""
    base_path = config["silver_layer_input"]["base_path"]
    data_types = config["silver_layer_input"]["refined_data_types"]
    date_folder = config["silver_layer_input"]["date_folder"]
    silver_layer_output = config["silver_layer_output"]

    dataframes = {}

    for data_type in data_types:
        input_path = os.path.join(base_path, data_type, date_folder)
        latest_file_path = get_latest_csv_file(input_path)

        if latest_file_path:
            dataframes[data_type] = spark.read.option("header", "true").csv(latest_file_path)
        else:
            print(f"No CSV files found in {input_path}, skipping...")

    # Transformation logic with duplicate column handling
    def clean_dataframe(df, entity_name):
        if df is not None:
            if "source" in df.columns:
                df = df.drop("source")
            df = df.withColumnRenamed("ingest_timestamp", f"{entity_name}_ingest_timestamp")
            df = df.withColumnRenamed("source_system", f"{entity_name}_source_system")
            df = df.dropDuplicates()
        return df

    customers_df = clean_dataframe(dataframes.get("refined_customers"), "customers")
    if customers_df is not None:
        customers_df = customers_df.withColumn("email", lower(col("email")))

    orders_df = clean_dataframe(dataframes.get("refined_orders"), "orders")
    if orders_df is not None:
        orders_df = orders_df.filter(col("total_amount") >= 0)

    order_items_df = clean_dataframe(dataframes.get("refined_order_items"), "order_items")
    if order_items_df is not None:
        order_items_df = order_items_df.filter(col("quantity") > 0)

    products_df = clean_dataframe(dataframes.get("refined_products"), "products")

    # Enforce Referential Integrity
    if order_items_df is not None and orders_df is not None:
        order_items_df = order_items_df.join(orders_df, "order_id", "inner")
    if orders_df is not None and customers_df is not None:
        orders_df = orders_df.join(customers_df, "customer_id", "inner")
    if products_df is not None and order_items_df is not None:
        products_df = products_df.join(order_items_df, "product_id", "inner")

    output_dfs = {
        'customers': customers_df,
        'orders': orders_df,
        'order_items': order_items_df,
        'products': products_df
    }

    for data_type, df in output_dfs.items():
        if df is not None:
            output_path = os.path.join(silver_layer_output, data_type, date_folder)
            os.makedirs(output_path, exist_ok=True)
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file_name = f"silver_{data_type}_{timestamp}.csv"
            df.write.mode("overwrite").option("header", "true").csv(os.path.join(output_path, output_file_name))
            print(f"Processed and saved: {data_type} to {os.path.join(output_path, output_file_name)}")


def main():
    """Main function to execute PySpark job."""
    config = load_config()
    spark = SparkSession.builder.appName("SilverLayerTransformation").getOrCreate()
    process_data(spark, config)
    spark.stop()


if __name__ == "__main__":
    main()
