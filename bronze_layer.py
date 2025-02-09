import os
import json
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


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
    """Read the latest CSV from each data type folder and write to bronze_layer with 'refined_' prefix."""
    base_path = config["bronze_layer_input"]["base_path"]
    data_types = config["bronze_layer_input"]["data_types"]
    bronze_layer_output = config["bronze_layer_output"]
    date_folder = config["bronze_layer_input"]["date_folder"]

    for data_type in data_types:
        input_path = os.path.join(base_path, data_type, date_folder)

        # Replace 'raw_' with 'refined_' for the output folder
        refined_data_type = data_type.replace("raw_", "refined_")
        output_path = os.path.join(bronze_layer_output, refined_data_type, date_folder)

        if not os.path.exists(input_path):
            print(f"Path does not exist: {input_path}")
            continue

        latest_file_path = get_latest_csv_file(input_path)

        if latest_file_path:
            df = spark.read.option("header", "true").csv(latest_file_path)

            # Add required columns
            df = df.withColumn("ingest_timestamp", current_timestamp())
            df = df.withColumn("source", lit("generated"))

            # Debugging - Print schema and sample data
            df.printSchema()
            df.show(5)

            # Ensure the output folder exists
            os.makedirs(output_path, exist_ok=True)

            # Reduce to one file and overwrite existing data
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

            print(f"Processed and saved: {latest_file_path} to {output_path}")
        else:
            print(f"No CSV files found in {input_path}, skipping...")


def main():
    """Main function to execute PySpark job."""
    config = load_config()

    spark = SparkSession.builder.appName("BronzeLayerIngestion").getOrCreate()
    process_data(spark, config)
    spark.stop()


if __name__ == "__main__":
    main()
