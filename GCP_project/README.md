# Data Pipeline Using Dataproc: Raw, Bronze, Silver, and Gold Layers

## Overview
This project implements a **multi-layered data pipeline** using Google Cloud Dataproc. The pipeline consists of four layers:
- **Raw Layer**: Stores raw data as ingested.
- **Bronze Layer**: Cleans and refines raw data.
- **Silver Layer**: Further processes, enriches, and joins datasets.
- **Gold Layer**: Optimized for analytics and reporting.

## Pipeline Architecture
```
Raw Layer     →    Bronze Layer    →    Silver Layer    →    Gold Layer
 (Raw Data)         (Cleansing)          (Transforming)        (Analytics Ready)
```

## Data Processing Flow
1. **Raw Layer**:
   - Stores raw files in Google Cloud Storage (GCS).
   - Data is unprocessed and directly ingested.

2. **Bronze Layer**:
   - Reads raw data from GCS.
   - Adds ingestion timestamps and metadata.
   - Writes transformed data back to GCS.

3. **Silver Layer**:
   - Joins, enriches, and applies transformations.
   - Ensures data consistency and integrity.
   - Writes refined datasets back to GCS.

4. **Gold Layer**:
   - Optimized for querying and reporting.
   - Aggregates and applies business logic.
   - Final processed data stored in GCS.

---

## Steps to Set Up and Run Using Git & Dataproc
### **1️⃣ Clone the Repository**
```bash
git clone https://github.com/yourusername/dataproc-pipeline.git
cd dataproc-pipeline
```

### **2️⃣ Upload Scripts to GCS**
```bash
gsutil cp bronze_layer.py silver_layer.py gold_layer.py gs://python-scripts-dataproc/
```

### **3️⃣ Submit Jobs to Dataproc**
Run each layer sequentially:

#### **Run Bronze Layer**
```bash
spark-submit gs://python-scripts-dataproc/bronze_layer.py
```

#### **Run Silver Layer**
```bash
spark-submit gs://python-scripts-dataproc/silver_layer.py
```

#### **Run Gold Layer**
```bash
spark-submit gs://python-scripts-dataproc/gold_layer.py
```

### **4️⃣ Commit and Push Changes to Git**
```bash
git add .
git commit -m "Updated Dataproc pipeline scripts"
git push origin main
```

---

## File Storage Structure
```plaintext
gs://raw-data-generated-by-script/
 ├── data/
 │   ├── raw_customers/
 │   │   ├── 20250221/customers_20250221_120000.csv
 │   ├── refined_customers/
 │   │   ├── silver_customers_20250221.csv
 │   ├── refined_orders/
 │   │   ├── silver_orders_20250221.csv
 │   ├── refined_order_items/
 │   │   ├── silver_order_items_20250221.csv
 │   ├── refined_products/
 │   │   ├── silver_products_20250221.csv
 │   ├── gold_customers/
 │   │   ├── gold_final_customers_20250221.csv
 │   ├── gold_orders/
 │   │   ├── gold_final_orders_20250221.csv
 │   ├── gold_order_items/
 │   │   ├── gold_final_order_items_20250221.csv
 │   ├── gold_products/
 │   │   ├── gold_final_products_20250221.csv
```

## Expected Outputs
After execution, the pipeline generates:
- **Bronze Layer Output**: `gs://raw-data-generated-by-script/data/refined_*`
- **Silver Layer Output**: `gs://raw-data-generated-by-script/data/silver_*`
- **Gold Layer Output**: `gs://raw-data-generated-by-script/data/gold_*`

---

## Troubleshooting
### **Check Logs in Dataproc**
If any job fails, check logs:
```bash
gcloud dataproc jobs list --region=<your-region>
gcloud dataproc jobs describe <job-id> --region=<your-region>
```

### **Check Output Files in GCS**
```bash
gsutil ls gs://raw-data-generated-by-script/data/gold_*
```

## Conclusion
This pipeline provides an **end-to-end data processing workflow** on Dataproc, leveraging **Spark and GCS** for scalable data processing. 🚀

