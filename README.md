# Wiki Lambda Analytics

## AWS Lambda Architecture for Real-Time Wikimedia Analytics

A cloud-based analytics platform that processes Wikimedia Recent Changes events using an AWS Lambda Architecture. The system combines real-time stream processing with historical batch analytics to identify trending articles by comparing live editing activity against historical article baselines.

---

## Project Overview

Wikipedia receives thousands of editing events continuously. Detecting unusual activity requires both:

- **Real-time processing** to identify current trends.
- **Historical analysis** to understand normal article behaviour.

This project implements a Lambda Architecture on AWS that integrates:

- A **Speed Layer** for near real-time analytics.
- A **Batch Layer** for historical baseline generation.
- A **Serving Layer** for dashboard visualization.

---

## Architecture

<img width="606" height="635" alt="image" src="https://github.com/user-attachments/assets/8605d67f-c865-4246-80fc-f834038b9373" />

---

# AWS Services Used

| Service | Purpose |
|---|---|
| Amazon Kinesis | Real-time Wikimedia event ingestion |
| Kinesis Firehose | Delivery of streaming data into S3 |
| Amazon S3 | Persistent storage layer |
| Amazon EMR | Spark batch and streaming processing |
| AWS Glue | Metadata cataloguing |
| Amazon Athena | Analytical querying |
| Amazon CloudWatch | Monitoring and performance metrics |
| Streamlit | Analytics dashboard |

---

# Speed Layer

The speed layer processes live Wikimedia events using Spark.

Features:

- Five-minute sliding window analytics.
- Real-time article edit counting.
- Trending article detection.
- Comparison against historical baselines.
- CloudWatch performance monitoring.

Metrics collected:

- Processing latency
- Window event count
- Ingestion rate
- Trending article count

Example output:

```
Events in Window: 328
Distinct Articles: 283
Trending Articles: 5
```

---

# Batch Layer

The batch layer processes historical Wikimedia data using PySpark on Amazon EMR.

Responsibilities:

- Historical event aggregation.
- Article edit frequency calculation.
- Baseline generation.
- Storage of analytical datasets in Parquet format.

The generated historical baselines are used by the speed layer to calculate abnormal activity.

---

# Trend Detection

Trending score:

```
Trend Score =
Current Edit Rate /
Historical Article Baseline
```

Interpretation:

| Score | Meaning |
|---|---|
| < 1 | Below normal activity |
| = 1 | Normal activity |
| > 1 | Increased activity |

---

# Dashboard

The Streamlit dashboard provides:

## Real-Time Analytics

- Current event activity
- Trending articles
- Sliding window statistics
- Processing latency

## Historical Analytics

- Total historical events
- Article statistics
- Most edited articles
- Baseline information

---

# Performance Evaluation

The system was evaluated using AWS CloudWatch metrics.

Generated measurements:

- Processing latency vs ingestion rate
- Processing latency vs window load
- Batch processing speedup
- Streaming throughput over time

Observed results:

- Average speed-layer processing latency remained below 300 ms after warm-up.
- Five-minute sliding windows handled increasing event volumes without significant degradation.
- Streaming ingestion remained stable throughout testing.

---

# Repository Structure

```
wiki-lambda-analytics/

├── aws/
│   └── spark/
│       ├── batch_layer_job.py
│       └── speed_layer_streaming.py
│
├── evidence/
│   ├── aws/
│   ├── graphs/
│   └── outputs/
│
├── app/
│   └── dashboard/
│
├── speed_layer_v9.py
├── batch_layer_job.py
├── requirements.txt
└── README.md
```

---

# Running the Dashboard

Install dependencies:

```bash
pip install -r requirements.txt
```

Run:

```bash
streamlit run app/dashboard/dashboard.py
```

---

# Research Objective

This project investigates:

> How can a Lambda Architecture implemented on AWS combine real-time stream processing and historical analytics to identify abnormal Wikimedia activity with low latency?

---

# Dataset

Source:

Wikimedia Recent Changes Stream

https://stream.wikimedia.org/

The dataset contains real-time Wikipedia editing events including:

- Article title
- User information
- Edit metadata
- Timestamp information

---

# Future Improvements

Possible extensions:

- Kubernetes-based deployment
- Automated anomaly detection models
- Larger-scale benchmarking
- Additional visualization capabilities

---

# Author

Pallavi Kandibilla

MSc Cloud Computing  
National College of Ireland
