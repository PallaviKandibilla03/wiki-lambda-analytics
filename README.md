# Real-Time Wikimedia Analytics Using AWS Lambda Architecture

## Overview

This project implements a cloud-based **Lambda Architecture** for analysing real-time Wikimedia editing activity using AWS services.

The system combines:

- **Speed Layer** → Real-time stream processing for detecting trending articles
- **Batch Layer** → Historical analytics and baseline generation
- **Serving Layer** → Combines real-time and historical insights for visualisation

The objective is to identify Wikipedia articles experiencing unusual editing activity by comparing current edit rates against historical behaviour.

The solution is implemented using AWS managed services including Amazon EC2, Kinesis, EMR, S3, Glue, Athena, and Streamlit.

---
The system follows a Lambda Architecture design:
<img width="606" height="635" alt="image" src="https://github.com/user-attachments/assets/896f56fc-2862-4f35-86cd-c25b9ef439ca" />

---

# Project Objectives

The main objectives of this project are:

- Build a scalable AWS Lambda Architecture for Wikimedia analytics.
- Ingest continuous Wikimedia events using Amazon Kinesis.
- Implement real-time stream processing using Apache Spark Streaming.
- Generate historical article baselines using PySpark.
- Detect trending articles using current activity compared with historical behaviour.
- Evaluate latency, throughput, and scalability of the cloud architecture.

---

# AWS Services Used

| Component | Technology |
|---|---|
| Data Source | Wikimedia Recent Changes EventStream |
| Compute | Amazon EC2 |
| Auto Scaling | EC2 Auto Scaling Group |
| Streaming Ingestion | Amazon Kinesis Data Streams |
| Data Delivery | Amazon Kinesis Firehose |
| Stream Processing | Apache Spark Streaming |
| Processing Platform | Amazon EMR |
| Batch Processing | PySpark |
| Storage | Amazon S3 |
| Metadata Management | AWS Glue Data Catalog |
| Query Engine | Amazon Athena |
| Monitoring | Amazon CloudWatch |
| Visualisation | Streamlit |

---

# System Components

## 1. Data Ingestion Layer

The producer application continuously collects events from the Wikimedia EventStream API.

Each event contains information such as:

- Article title
- Edit timestamp
- Namespace
- User information
- Edit metadata

The Python producer application:

1. Connects to Wikimedia EventStream.
2. Parses incoming JSON events.
3. Publishes events into Amazon Kinesis Data Streams.

The producer runs on Amazon EC2 and is deployed using an Auto Scaling Group.

---

## 2. Speed Layer - Real-Time Processing

The speed layer provides low-latency analytics over incoming Wikimedia events.

**Implementation:**
- Apache Spark Streaming
- Amazon EMR
- Amazon Kinesis Data Streams

**Features:**
- Five-minute sliding window processing.
- Real-time event aggregation.
- Article activity calculation.
- Trending article detection.

## 3. Batch Layer - Historical Analytics

The batch layer generates historical context required for trend detection.

**Implementation:**
- Apache Spark
- PySpark
- Amazon EMR
- Amazon S3

**The batch layer calculates:**
- Article edit baselines
- Hourly edit rates
- Top edited articles
- Namespace statistics
- Bot versus human activity

Processed datasets are stored in Amazon S3 and registered using AWS Glue Data Catalog.

**Available Athena tables:**
- `article_baseline`
- `batch_summary`
- `bot_vs_human`
- `edit_rate_hourly`
- `namespace_breakdown`
- `top_articles`

---

## 4. Serving and Visualisation Layer

The serving layer combines:
- Real-time speed-layer outputs
- Historical batch analytics

**The Streamlit dashboard provides:**
- **Real-Time Insights:** Current event activity, trending articles, sliding window statistics, latest processing results.
- **Historical Insights:** Top edited articles, historical baselines, article activity trends, analytical summaries.

---

# Auto Scaling Configuration

The producer component uses an EC2 Auto Scaling Group.

**Configuration:**
- **Minimum Instances:** 1
- **Maximum Instances:** 4
- **Instance Type:** `t3.micro`

Scaling policies are configured using AWS Auto Scaling target tracking.

**Benefits:**
- Handles increased ingestion workload.
- Maintains availability.
- Reduces unnecessary resource usage during low traffic periods.

---
---

# Performance Evaluation

The system was evaluated using:
- Processing latency
- Event throughput
- Sliding-window workload
- Spark executor scaling

**Observed behaviour:**
- Processed more than 1000 Wikimedia events per five-minute window.
- Maintained sub-second processing latency.
- Generated trending article results continuously.
- Successfully integrated batch and streaming analytics.

Scalability Analysis
Components that scaled effectively
Amazon Kinesis: Provided reliable ingestion and buffering between producers and processors.

Amazon S3: Provided scalable storage for raw events, processed datasets, and analytical outputs.

Independent Lambda Layers: The separation between batch and speed layers allowed independent processing without affecting real-time analytics.

Limitations
AWS Learner Lab resource restrictions limited large-scale benchmarking.

Historical baselines were available only for articles with sufficient history.

Large EMR cluster scaling experiments were not possible.

Future Improvements
Continuous updating of article baselines.

Machine learning based anomaly detection.

Larger EMR cluster benchmarking.

Automated infrastructure deployment using Terraform or CloudFormation.

Improved monitoring and alerting.

Multi-region deployment for higher availability.

Repository Structure
Plaintext
wiki-lambda-analytics/
│
├── producer/
│   └── kinesis_producer.py
│
├── speed_layer/
│   └── spark_streaming.py
│
├── batch_layer/
│   └── pyspark_jobs/
│
├── dashboard/
│   └── streamlit_dashboard.py
│
├── docs/
│   ├── architecture/
│   │   └── architecture.png
│   │
│   └── screenshots/
│
├── requirements.txt
│
└── README.md
Technologies
Python

Apache Spark

PySpark

Spark Streaming

Amazon EC2

Amazon Kinesis

Amazon EMR

Amazon S3

AWS Glue

Amazon Athena

Streamlit

Project Demonstration
Dashboard
(Add screenshots here)

Performance Evaluation
(Add screenshots here)

Authors
Cloud Computing Project

National College of Ireland

MSc in Cloud Computing

License
This project was developed for academic purposes.
