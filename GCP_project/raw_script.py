import csv
import random
import datetime
import json
from google.cloud import storage

DEFAULT_CONFIG_FILE = "gs://python-scripts-dataproc/config.json"


def load_config(config_file=DEFAULT_CONFIG_FILE):
    """Loads configuration from a JSON file stored in GCS."""
    default_config = {
        "gcs_bucket": "raw-data-generated-by-script",
        "paths": {
            "output_folder": "data",
            "tracker_file": "data/id_tracker.json"
        },
        "data_settings": {
            "num_rows_per_table": 5000
        }
    }

    try:
        if config_file.startswith("gs://"):
            bucket_name = config_file.split("gs://")[1].split("/")[0]
            blob_name = "/".join(config_file.split("gs://")[1].split("/")[1:])
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            config_data = blob.download_as_text()
            user_config = json.loads(config_data)
            default_config.update(user_config)
    except Exception as e:
        print(f"Warning: Could not read config. Using defaults. Error: {e}")

    return default_config


def generate_data(num_rows, start_id):
    """Generate dummy data."""
    return [[i, f"Item {i}", random.randint(1, 100)] for i in range(start_id, start_id + num_rows)]


def write_to_gcs(bucket_name, folder, file_name, header, rows):
    """Write data to GCS as CSV, ensuring correct folder structure."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Ensure date-based folder structure is enforced
    blob_path = f"{folder}/{file_name}"

    # Prepare CSV data
    csv_data = "\n".join([",".join(map(str, row)) for row in [header] + rows])

    blob = bucket.blob(blob_path)
    blob.upload_from_string(csv_data, content_type="text/csv")

    print(f"✅ Saved {file_name} to gs://{bucket_name}/{blob_path}")


def main():
    config = load_config()
    bucket_name = config["gcs_bucket"]
    num_rows = config["data_settings"]["num_rows_per_table"]

    # Ensure correct date folder is included
    run_date = datetime.date.today().strftime("%Y%m%d")
    file_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Define correct GCS folder structure
    base_folder = "data"
    folders = {
        "customers": f"{base_folder}/raw_customers/{run_date}",
        "orders": f"{base_folder}/raw_orders/{run_date}",
        "order_items": f"{base_folder}/raw_order_items/{run_date}",
        "products": f"{base_folder}/raw_products/{run_date}"
    }

    # Generate data
    customers_data = generate_data(num_rows, 1)
    orders_data = generate_data(num_rows, 1)
    order_items_data = generate_data(num_rows, 1)
    products_data = generate_data(num_rows, 1)

    # Write files ensuring correct paths
    write_to_gcs(bucket_name, folders["customers"], f"customers_{file_timestamp}.csv",
                 ["customer_id", "name", "value"], customers_data)

    write_to_gcs(bucket_name, folders["orders"], f"orders_{file_timestamp}.csv",
                 ["order_id", "name", "value"], orders_data)

    write_to_gcs(bucket_name, folders["order_items"], f"order_items_{file_timestamp}.csv",
                 ["order_item_id", "name", "value"], order_items_data)

    write_to_gcs(bucket_name, folders["products"], f"products_{file_timestamp}.csv",
                 ["product_id", "name", "value"], products_data)

    print("✅ Data Generation Completed.")


if __name__ == "__main__":
    main()
