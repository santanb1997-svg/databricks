# Databricks notebook source
# ============================================================
# Databricks DLT Pipeline: Bronze → Silver → Gold + Quarantine
# ============================================================

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# CONFIG
LANDING_DIR = "/Volumes/nyc_yellow_taxi_trip/landing/yellow_taxi_data/"

taxi_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True),
    StructField("pickup_zone", StringType(), True),
    StructField("dropoff_zone", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("trip_duration", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("driver_id", StringType(), True)
])

# BRONZE TABLE
@dlt.table(name="nyc_yellow_taxi_trip.taxi_bronze.taxi_bronze")
def taxi_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{LANDING_DIR}/_schema_location")
        .schema(taxi_schema)
        .load(LANDING_DIR)
    )
    return df.withColumn("_bronze_ingest_time", current_timestamp()) \
             .withColumn("_source_file", col("_metadata.file_path"))

# QUARANTINE TABLE
@dlt.table(name="nyc_yellow_taxi_trip.taxi_bronze.taxi_quarantined_records")
def taxi_quarantined_records():
    df = dlt.read_stream("nyc_yellow_taxi_trip.taxi_bronze.taxi_bronze")
    reason = concat_ws(";",
        when(col("pickup_datetime").isNull(), "missing_pickup"),
        when(col("dropoff_datetime").isNull(), "missing_dropoff"),
        when((col("trip_distance").isNull()) | (col("trip_distance") <= 0), "invalid_trip_distance"),
        when((col("fare_amount").isNull()) | (col("fare_amount") < 0), "invalid_fare")
    )
    return df.withColumn("error_reason", reason) \
             .filter(col("error_reason") != "") \
             .withColumn("quarantine_id", expr("uuid()")) \
             .withColumn("quarantined_at", current_timestamp()) \
             .select("quarantine_id", "error_reason", "trip_id", "pickup_datetime",
                     "dropoff_datetime", "_source_file", "quarantined_at")

# SILVER TABLE
@dlt.table(name="nyc_yellow_taxi_trip.taxi_silver.taxi_silver")
@dlt.expect_or_drop("pickup_not_null", "pickup_datetime IS NOT NULL")
@dlt.expect_or_drop("dropoff_not_null", "dropoff_datetime IS NOT NULL")
@dlt.expect_or_drop("positive_distance", "trip_distance > 0")
@dlt.expect_or_drop("valid_fare", "fare_amount >= 0")
def taxi_silver():
    df = dlt.read_stream("nyc_yellow_taxi_trip.taxi_bronze.taxi_bronze")
    df = df.withColumn("pickup_ts", to_timestamp("pickup_datetime")) \
           .withColumn("dropoff_ts", to_timestamp("dropoff_datetime")) \
           .withColumn("ride_time_min", (col("dropoff_ts").cast("long") - col("pickup_ts").cast("long")) / 60) \
           .withColumn("is_long_trip", col("trip_distance") > 10) \
           .withColumn("avg_speed_kmh", when(col("ride_time_min") > 0, col("trip_distance") / (col("ride_time_min") / 60)).otherwise(0)) \
           .withColumn("_silver_ingest_time", current_timestamp())
    return df

# GOLD TABLE (KPIs)
@dlt.table(name="nyc_yellow_taxi_trip.taxi_gold.taxi_gold")
def taxi_kpis():
    s = dlt.read_stream("nyc_yellow_taxi_trip.taxi_silver.taxi_silver")
    kpis = s.groupBy(window("pickup_ts", "1 hour"), "pickup_zone") \
            .agg(count("*").alias("total_trips"),
                 avg("fare_amount").alias("avg_fare"),
                 sum("fare_amount").alias("total_revenue"),
                 avg("trip_distance").alias("avg_distance"),
                 avg("ride_time_min").alias("avg_duration_min"),
                 avg("avg_speed_kmh").alias("avg_speed_kmh")) \
            .withColumn("_gold_ingest_time", current_timestamp()) \
            .withColumn("window_start", col("window").start) \
            .withColumn("window_end", col("window").end) \
            .drop("window")
    return kpis
