# Databricks notebook source
# ============================================================
#          FINAL ORCHESTRATION NOTEBOOK (SAFE VERSION)
#     Logs SUCCESS + FAILURE for DLT, Bronze, Silver, Gold
# ============================================================

from pyspark.sql.functions import *
from datetime import datetime
import traceback, uuid

# ------------------------------------------------------------
# PARAMETERS
# ------------------------------------------------------------
dbutils.widgets.text("SLACK_WEBHOOK", "")
SLACK_WEBHOOK = dbutils.widgets.get("SLACK_WEBHOOK")

LOG_TABLE = "nyc_yellow_taxi_trip.log_yellow_taxi.pipeline_logs"
DLT_PIPELINE_ID = "58c390af-9f93-439f-b842-7f1f10db9d9d"

# ------------------------------------------------------------
# CREATE LOG TABLE IF NEEDED
# ------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS nyc_yellow_taxi_trip")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
  RunId STRING,
  Timestamp TIMESTAMP,
  Layer STRING,
  Status STRING,
  ProcessedRecordCount LONG,
  ErrorMessages STRING,
  FileName STRING
) USING delta
""")

# ------------------------------------------------------------
# LOGGING FUNCTION
# ------------------------------------------------------------
def write_log(layer, status, processed=0, error_msg="", file_name=""):
    row = (
        str(uuid.uuid4()),
        datetime.now(),
        layer,
        status,
        int(processed) if processed else 0,
        error_msg,
        file_name
    )
    df = spark.createDataFrame(
        [row],
        ["RunId","Timestamp","Layer","Status","ProcessedRecordCount","ErrorMessages","FileName"]
    )
    try:
        df.write.format("delta").mode("append").saveAsTable(LOG_TABLE)
    except Exception as e:
        print(f"[LOG ERROR] Failed to write log: {e}")
    print(f"[LOG] {layer} → {status}")

# ------------------------------------------------------------
# SLACK ALERT (OPTIONAL)
# ------------------------------------------------------------
def send_alert(msg):
    if SLACK_WEBHOOK.strip():
        try:
            import requests
            requests.post(SLACK_WEBHOOK, json={"text": msg})
        except:
            print("[ALERT] Slack alert failed")

# ------------------------------------------------------------
# READ DLT PIPELINE STATUS FROM system.events (SAFE)
# ------------------------------------------------------------
def log_dlt_pipeline_status():
    try:
        # Check if the table exists
        if spark.catalog._jcatalog.tableExists("system.events"):
            events = spark.read.table("system.events")
            last = (
                events.filter(
                    (col("event_type") == "pipeline_progress") &
                    (col("pipeline_id") == DLT_PIPELINE_ID)
                )
                .orderBy(col("timestamp").desc())
                .limit(1)
                .collect()
            )

            if not last:
                write_log("DLT_PIPELINE", "FAILED", error_msg="No DLT events found for pipeline ID")
                return "FAILED"

            details = last[0]["details"]
            status = details.get("status", "UNKNOWN")

            if status == "COMPLETED":
                write_log("DLT_PIPELINE", "SUCCESS")
                return "SUCCESS"
            else:
                error_msg = details.get("message", "Unknown DLT failure")
                write_log("DLT_PIPELINE", "FAILED", error_msg=error_msg)
                return "FAILED"
        else:
            # Table not found → skip DLT status
            write_log("DLT_PIPELINE", "SKIPPED", error_msg="system.events table not found")
            return "SKIPPED"

    except Exception as e:
        errmsg = traceback.format_exc()
        write_log("DLT_PIPELINE", "FAILED", error_msg=errmsg)
        return "FAILED"

# ------------------------------------------------------------
# VALIDATE EACH LAYER
# ------------------------------------------------------------
def validate_layer(layer, table_name):
    try:
        if not spark.catalog._jcatalog.tableExists(table_name):
            write_log(layer, "FAILED", error_msg=f"Table {table_name} does not exist")
            return None

        df = spark.table(table_name)
        count_rows = df.count()

        if "_source_file" in df.columns:
            files = df.groupBy("_source_file").agg(count("*").alias("rows")).collect()
            for row in files:
                write_log(layer, "SUCCESS", processed=row["rows"], file_name=row["_source_file"])
        else:
            write_log(layer, "SUCCESS", processed=count_rows)

        return count_rows

    except Exception as e:
        errmsg = traceback.format_exc()
        write_log(layer, "FAILED", error_msg=errmsg)
        return None

# ------------------------------------------------------------
# MAIN ORCHESTRATION EXECUTION
# ------------------------------------------------------------
write_log("ORCHESTRATION", "STARTED")
print("🚀 ORCHESTRATION STARTED")

# 1) DLT pipeline status
pipeline_status = log_dlt_pipeline_status()

# 2) Validate Bronze / Silver / Gold
bronze = validate_layer("BRONZE", "nyc_yellow_taxi_trip.taxi_bronze.taxi_bronze")
silver = validate_layer("SILVER", "nyc_yellow_taxi_trip.taxi_silver.taxi_silver")
gold   = validate_layer("GOLD",   "nyc_yellow_taxi_trip.taxi_gold.taxi_gold")

# 3) Final log
write_log("ORCHESTRATION_END", "SUCCESS")
print("✅ ORCHESTRATION COMPLETED")
