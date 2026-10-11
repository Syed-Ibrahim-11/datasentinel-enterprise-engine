# Databricks notebook source
# ============================================================
# DATASENTINEL | CELL 1: Setup & Autonomous Ingestion (Bronze Layer)
# Action: Reads raw enterprise data and establishes environment variables.
# ============================================================

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# ==========================================================
# 🛠️ WORKER CONFIGURATION: CHANGE THESE FOR YOUR CUSTOM DATASET
# ==========================================================
CATALOG_NAME = "pyspark_lab"
SCHEMA_NAME = "datasentinel"
FILE_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/raw_files/datasentinel_raw_customers.csv"

# Define your unique key and critical column requirements:
PRIMARY_KEY = "customer_id"
REQUIRED_COLS = ["name", "email"]
PII_COLS = ["email"]  # Sensitive columns to mask for compliance

# Autonomous ingestion from Unity Catalog volume
raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(FILE_PATH)
)

print(f"-> [SUCCESS] Ingested raw dataset successfully.")
print(f"-> Total Rows Loaded   : {raw_df.count()}")
print(f"-> Total Columns Found : {len(raw_df.columns)}")
display(raw_df.limit(3))

# COMMAND ----------

# ============================================================
# DATASENTINEL | CELL 2: PII Masking & GDPR/CCPA Compliance
# Action: Anonymizes sensitive personal data columns.
# ============================================================

print("=" * 60)
print("DATASENTINEL | PII COMPLIANCE MASKING ENGINE")
print("=" * 60)

masked_df = raw_df

# Apply secure cryptographic hashing to designated PII columns
for col_name in PII_COLS:
    if col_name in masked_df.columns:
        masked_df = masked_df.withColumn(col_name, F.sha2(F.col(col_name), 256))
        print(f"-> [MASKED] Privacy protection applied to column: '{col_name}'")
    else:
        print(f"-> [SKIPPED] PII column '{col_name}' not found in current schema.")

print("=" * 60)
display(masked_df.limit(3))

# COMMAND ----------

# ============================================================
# DATASENTINEL | CELL 3: Smart Auto-Remediation & Cleansing
# Action: Normalizes string formats, trims whitespace, and auto-corrects text casing.
# ============================================================

print("=" * 60)
print("DATASENTINEL | SMART AUTO-REMEDIATION ENGINE")
print("=" * 60)

remediated_df = masked_df

# Dynamically normalize all string columns (excluding primary keys and hashes)
for col_name, data_type in masked_df.dtypes:
    if data_type == "string" and col_name != PRIMARY_KEY and col_name not in PII_COLS:
        remediated_df = remediated_df.withColumn(
            col_name, 
            F.initcap(F.trim(F.col(col_name)))
        )

print("-> [SUCCESS] String casing normalized and trailing spaces trimmed across dataset.")
display(remediated_df.limit(3))x`

# COMMAND ----------

# ============================================================
# DATASENTINEL | CELL 4: Automated Health Profiling & Diagnostics
# Action: Scans for nulls, duplicate keys, and computes a data health score.
# ============================================================

print("=" * 60)
print("DATASENTINEL | HEALTH PROFILING & DIAGNOSTICS")
print("=" * 60)

total_rows = remediated_df.count()

# Set up window partition for duplicate detection
window_spec = Window.partitionBy(PRIMARY_KEY).orderBy(PRIMARY_KEY)
staged_df = remediated_df.withColumn("_row_num", F.row_number().over(window_spec))

# Calculate defects
duplicate_count = staged_df.filter(F.col("_row_num") > 1).count()
null_primary_count = staged_df.filter(F.col(PRIMARY_KEY).isNull()).count()

# Compute Health Score percentage
total_defects = duplicate_count + null_primary_count
health_score = max(0.0, 100.0 - ((total_defects / total_rows) * 100))

print(f"-> Total Incoming Rows   : {total_rows}")
print(f"-> Primary Key Collisions: {duplicate_count}")
print(f"-> Missing Primary Keys  : {null_primary_count}")
print(f"-> Dataset Health Score  : {health_score:.2f}%")
print("=" * 60)

# COMMAND ----------

# ============================================================
# DATASENTINEL | CELL 5: Quarantine Routing & Delta Persistence
# Action: Separates valid rows from defective rows and saves to Silver tables.
# ============================================================

print("=" * 60)
print("DATASENTINEL | PERSISTENCE & QUARANTINE ROUTING")
print("=" * 60)

# Build strict rule conditions for invalid rows
invalid_condition = (F.col(PRIMARY_KEY).isNull()) | (F.col("_row_num") > 1)

# Stream split
clean_df = staged_df.filter(~invalid_condition).drop("_row_num")
quarantine_df = staged_df.filter(invalid_condition).drop("_row_num")

# Target Delta table paths
clean_table = f"{CATALOG_NAME}.{SCHEMA_NAME}.clean_customers"
ledger_table = f"{CATALOG_NAME}.{SCHEMA_NAME}.issue_ledger"

# Write to Delta Lake
clean_df.write.format("delta").mode("overwrite").saveAsTable(clean_table)
quarantine_df.write.format("delta").mode("overwrite").saveAsTable(ledger_table)

print(f"-> Clean Records Saved   : {clean_df.count()} -> Table: {clean_table}")
print(f"-> Quarantined Issues    : {quarantine_df.count()} -> Table: {ledger_table}")

# COMMAND ----------

# ============================================================
# DATASENTINEL | CELL 6: Executive Corporate Report Generator
# Action: Generates curated business intelligence metrics for leadership.
# ============================================================

print("=" * 60)
print("DATASENTINEL | EXECUTIVE CORPORATE REPORTING")
print("=" * 60)

# Read from clean Silver table
df_clean = spark.read.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.clean_customers")

# Standardize dimensions and aggregate metrics
executive_report = df_clean \
    .withColumn("dimension_city", F.initcap(F.trim(F.coalesce(F.col("city"), F.lit("Unknown"))))) \
    .groupBy("dimension_city") \
    .agg(
        F.count(PRIMARY_KEY).alias("total_customers"),
        F.round(F.avg("annual_spend"), 2).alias("average_annual_spend"),
        F.round(F.sum("annual_spend"), 2).alias("total_revenue_impact")
    ) \
    .orderBy(F.desc("total_revenue_impact"))

display(executive_report)

# Save Gold executive report table
gold_table = f"{CATALOG_NAME}.{SCHEMA_NAME}.executive_summary_report"
executive_report.write.format("delta").mode("overwrite").saveAsTable(gold_table)
print(f"-> Executive summary report saved to: {gold_table}")

# COMMAND ----------

# ============================================================
# DATASENTINEL | CELL 7: SLA Circuit Breaker & Telemetry
# Action: Validates pass rates against minimum enterprise standards.
# ============================================================

print("=" * 60)
print("DATASENTINEL | SLA CIRCUIT BREAKER GATE")
print("=" * 60)

pass_rate = (clean_df.count() / total_rows) * 100
MIN_SLA_THRESHOLD = 90.0

print(f"-> Final Batch Pass Rate  : {pass_rate:.2f}%")
print(f"-> Required Minimum SLA   : {MIN_SLA_THRESHOLD}%")

if pass_rate < MIN_SLA_THRESHOLD:
    print("\n[CRITICAL WARNING] Data quality breached company SLA threshold!")
    print("[ACTION] Pipeline flagged. Quarantine ledger inspection required.")
else:
    print("\n[SUCCESS] Pipeline passed all enterprise quality gates autonomously.")