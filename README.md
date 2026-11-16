# DWBDA Assignment 4: Apache Spark Structured Streaming with Deduplication & Checkpointing

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/jits77111-source/DWBDA-Assignment-4-Spark-Structured-Streaming/blob/main/Assignment_4_Spark_Structured_Streaming.ipynb)

**Author:** Jit Sarkar  
**Course:** Data Warehousing, Big Data & Analytics  
**Implementation:** Apache Spark Structured Streaming, Event Deduplication, Fault-Tolerant Checkpointing  

---

## 📌 Problem Overview
Demonstrates real-time data stream processing using **Apache Spark Structured Streaming** on streaming JSON file sources.

### Key Capabilities Demonstrated:
1. **Streaming Data Source**: Continuously ingests streaming JSON files from a designated directory.
2. **Schema Enforcement**: Uses `StructType` to strictly type event telemetry payloads.
3. **Deduplication**: Uses `.dropDuplicates(["event_id"])` to discard duplicate transmissions.
4. **Fault Tolerance & Checkpointing**: Implements Write-Ahead Logs (WAL) via `checkpointLocation` to enable stateful failure recovery and exactly-once execution.

---

## 📂 Repository Contents
```
DWBDA-Assignment-4-Spark-Structured-Streaming/
├── Assignment_4_Spark_Structured_Streaming.ipynb  # Interactive Jupyter Notebook (Colab Ready)
├── assignment_4_streaming.py                     # Standalone executable streaming script
├── sample_stream_data/                           # Sample JSON micro-batches
├── .gitignore                                    # Git ignore file
└── README.md                                     # Documentation
```

---

## 🚀 How to Run

### Method 1: 1-Click Run in Google Colab (Recommended)
Click the badge above or visit:  
👉 [Open in Google Colab](https://colab.research.google.com/github/jits77111-source/DWBDA-Assignment-4-Spark-Structured-Streaming/blob/main/Assignment_4_Spark_Structured_Streaming.ipynb)

### Method 2: Run Locally
```bash
# 1. Clone repository
git clone https://github.com/jits77111-source/DWBDA-Assignment-4-Spark-Structured-Streaming.git
cd DWBDA-Assignment-4-Spark-Structured-Streaming

# 2. Install dependencies
pip install pyspark

# 3. Execute streaming script
python assignment_4_streaming.py
```
