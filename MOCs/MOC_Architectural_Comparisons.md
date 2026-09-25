---
title: "MOC: Cross-Engine Architectural Comparisons"
type: moc
tags:
  - moc
  - comparisons
  - benchmarks
  - architecture-decisions
aliases:
  - Comparisons MOC
---

# ⚖️ MOC: Cross-Engine Architectural Comparisons

> Map of Content for side-by-side technical evaluations, benchmark considerations, and architectural decision frameworks across competing big data technologies.

Parent: [[MOCs/00_Root_Streamhouse_MOC|Root Streamhouse MOC]]

---

## 📑 Dedicated Comparative Analyses

### 1. 🔍 [[Comparisons/Trino_vs_StarRocks|Trino vs. StarRocks: Federated Data Lake SQL vs. Real-Time OLAP]]
- **Core Trade-off:** Pure Compute-Only Federation vs. High-Concurrency Sub-Second Vectorized OLAP.
- **Key Dimensions Evaluated:**
  - JVM Bytecode Generation vs. C++ SIMD AVX-512 Vectorization.
  - Connector SPI vs. Native C++ Readers with Local NVMe Data Caching.
  - Cost-Based Join Reordering vs. Automated Materialized View Query Rewrite.
  - Concurrency scaling (10s of users vs. 10,000+ QPS).

### 2. 🧊 [[Comparisons/Table_Formats_Iceberg_vs_Paimon_vs_Hive|Lakehouse Table Formats: Apache Iceberg vs. Apache Paimon vs. Apache Hive]]
- **Core Trade-off:** Universal Open Lakehouse Standard vs. Streaming-First LSM Engine vs. Legacy Directory Catalog.
- **Key Dimensions Evaluated:**
  - Directory-as-partition ($O(N)$ S3 LIST) vs. Snapshot Manifest Tree ($O(1)$ local read) vs. Multi-Level LSM-Tree.
  - Streaming CDC upserts: Positional/Equality deletes (Iceberg) vs. LSM MemTable flush (Paimon).
  - Hidden Partitioning and Schema Evolution safety guarantees.
  - Native Changelog Generation replacing intermediate Kafka topics.

---

## 🧭 Technology Decision Trees

### Query & Serving Layer Decision
- Need to query across 5+ disparate databases without moving data? $\to$ **[[Trino/01_Overview|Trino]]**
- Need sub-50ms dashboard response times under thousands of concurrent users? $\to$ **[[StarRocks/01_Overview|StarRocks]]**
- Need structured, version-controlled batch data modeling? $\to$ **[[dbt/01_Overview|dbt]]**

### Storage & Lakehouse Format Decision
- Building an open enterprise data lakehouse for multi-engine access (Spark, Trino, Snowflake)? $\to$ **[[Apache Iceberg/01_Overview|Apache Iceberg]]**
- High-throughput streaming CDC upserts with continuous downstream Flink consumption? $\to$ **[[Apache Paimon/01_Overview|Apache Paimon]]**
- Maintaining metadata for legacy Hadoop clusters? $\to$ **[[Apache Hive/01_Overview|Apache Hive (HMS)]]**
