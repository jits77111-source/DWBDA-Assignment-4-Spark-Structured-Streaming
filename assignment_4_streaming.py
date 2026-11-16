"""
Assignment 4: Apache Spark Structured Streaming with Deduplication & Checkpointing
Author: Jit Sarkar
Repository: DWBDA-Assignment-4-Spark-Structured-Streaming
Description: Implements real-time stream processing on IoT Vehicle Telemetry data,
             applying dropDuplicates() and fault-tolerant checkpointing.
"""

import os
import shutil
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def main():
    print("=" * 70)
    print("DWBDA Assignment 4: Apache Spark Structured Streaming")
    print("=" * 70)

    # 1. Directories setup
    stream_input = os.path.abspath("streaming_input_source")
    checkpoint_dir = os.path.abspath("streaming_checkpoint")

    shutil.rmtree(stream_input, ignore_errors=True)
    shutil.rmtree(checkpoint_dir, ignore_errors=True)
    os.makedirs(stream_input, exist_ok=True)
    os.makedirs(checkpoint_dir, exist_ok=True)

    # 2. Initialize SparkSession
    spark = SparkSession.builder \
        .appName("IoTStructuredStreaming") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print(f"\nApache Spark Session Started (Version: {spark.version})")

    # 3. Define Strict Schema
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("speed_kmh", DoubleType(), True),
        StructField("fuel_pct", DoubleType(), True),
        StructField("status", StringType(), True)
    ])
    print("Streaming Schema Enforced:", schema)

    # 4. Create Streaming Source
    stream_df = spark.readStream \
        .schema(schema) \
        .json(stream_input)

    # 5. Deduplication using dropDuplicates
    unique_stream_df = stream_df.dropDuplicates(["event_id"])
    print("[SUCCESS] Duplicate elimination rule configured on 'event_id'.")

    # 6. Start Streaming Query to Console with Checkpoint
    query = unique_stream_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime="2 seconds") \
        .start()

    print("\n[STREAM ACTIVE] Writing micro-batch files to simulate real-time ingestion...")

    # Write Batch 1
    batch1 = [
        {"event_id": "EVT_101", "device_id": "TRUCK_01", "timestamp": "2026-03-01 10:00:00", "speed_kmh": 72.5, "fuel_pct": 85.0, "status": "NORMAL"},
        {"event_id": "EVT_102", "device_id": "TRUCK_02", "timestamp": "2026-03-01 10:00:01", "speed_kmh": 94.2, "fuel_pct": 42.0, "status": "SPEEDING"}
    ]
    with open(os.path.join(stream_input, "file1.json"), "w") as f:
        for r in batch1: f.write(json.dumps(r) + "\n")
    print("--> Ingested file1.json (2 records)")
    time.sleep(4)

    # Write Batch 2 with Duplicate
    batch2 = [
        {"event_id": "EVT_102", "device_id": "TRUCK_02", "timestamp": "2026-03-01 10:00:01", "speed_kmh": 94.2, "fuel_pct": 42.0, "status": "SPEEDING"}, # Duplicate!
        {"event_id": "EVT_103", "device_id": "TRUCK_03", "timestamp": "2026-03-01 10:00:05", "speed_kmh": 61.0, "fuel_pct": 18.0, "status": "LOW_FUEL"}
    ]
    with open(os.path.join(stream_input, "file2.json"), "w") as f:
        for r in batch2: f.write(json.dumps(r) + "\n")
    print("--> Ingested file2.json (1 duplicate + 1 new record)")
    time.sleep(4)

    # Await query completion
    query.processAllAvailable()
    query.stop()
    print("\n[STREAM FINISHED] Micro-batches processed cleanly with duplicate detection and checkpointing.")
    spark.stop()

if __name__ == "__main__":
    main()
