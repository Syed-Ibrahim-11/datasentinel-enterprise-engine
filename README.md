# DataSentinel: Autonomous Enterprise Data Quality & Ingestion Engine

**DataSentinel** is an enterprise-grade, autonomous Data Quality & Ingestion framework built with **PySpark, Delta Lake, and Databricks Unity Catalog**. It is designed to solve real-world data engineering challenges by bridging the gap between messy raw landing data and clean, compliant, executive-ready analytical tables.

---

## 🏗️ Architecture & Pipeline Flow
DataSentinel follows a modern **Medallion Architecture** combined with automated data trust and governance layers:

1. **Bronze Layer (Ingestion):** Ingests raw CSV or Parquet files autonomously from Unity Catalog volumes.
2. **Privacy & Cleansing Layer:** Applies GDPR/CCPA cryptographic PII masking and smart auto-remediation (string casing normalization and whitespace trimming).
3. **Silver Layer (Validation & Quarantine):** Performs deep health profiling, isolates anomalies and duplicate primary keys into an immutable **Issue Ledger** audit trail, and persists valid records into clean Delta tables.
4. **Gold Layer (Executive Analytics):** Aggregates clean data into curated business intelligence reports for leadership dashboards.
5. **SLA Circuit Breaker:** Acts as an automated quality gate that prevents corrupted data from poisoning downstream dashboards.

---

## 🚀 Key Enterprise Features

* **Universal Customizability:** Easily adaptable to *any* company dataframe by modifying configuration variables at the top of the notebook.
* **GDPR/CCPA PII Masking:** Automatically secures sensitive personal data columns (like emails) using secure SHA-256 cryptographic hashing.
* **Smart Auto-Remediation:** Strips trailing whitespaces and normalizes string casing (`initcap`) on the fly to eliminate common manual data entry bugs.
* **Deep Health Profiling:** Scans schemas for primary key collisions and missing values, calculating real-time **Data Health Score percentages**.
* **Quarantine Audit Trails:** Separates bad records into a dedicated `issue_ledger` table so data quality issues can be tracked without breaking pipeline execution.
* **Executive Corporate Reporting:** Automatically generates standard business metric summaries (total customers, average spend, revenue impact) grouped by key dimensions.
* **Circuit Breaker SLA Gate:** Halts or flags workflows if data quality drops below enterprise minimum service level agreements (e.g., 90%).

---

## 🛠️ Worker Guide: How to Use Your Own Dataset

Every enterprise works with different schemas. To adapt DataSentinel for your own custom dataset, simply update the configuration block in **Cell 1** of the notebook:

```python
# Change these variables to match your company's schema:
CATALOG_NAME = "pyspark_lab"
SCHEMA_NAME = "datasentinel"
FILE_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/raw_files/your_custom_file.csv"

PRIMARY_KEY = "your_unique_id_column"
REQUIRED_COLS = ["column_a", "column_b"]
PII_COLS = ["email", "phone_number"]  # Columns requiring privacy masking
