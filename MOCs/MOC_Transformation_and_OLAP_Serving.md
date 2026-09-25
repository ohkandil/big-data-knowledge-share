---
title: "MOC: Transformation & OLAP Serving"
type: moc
tags:
  - moc
  - transformation
  - olap
  - query-engine
  - dbt
  - trino
  - starrocks
aliases:
  - Transformation MOC
  - Query Engines MOC
---

# 📊 MOC: Transformation & OLAP Serving

> Map of Content for SQL data transformation, analytics engineering, federated query engines, and sub-second real-time OLAP serving platforms.

Parent: [[MOCs/00_Root_Streamhouse_MOC|Root Streamhouse MOC]]

---

## 🏗️ Core Technologies in this Domain

### 1. 🟠 [[dbt/01_Overview|dbt (data build tool)]] (Analytics Engineering & ELT)
- **Primary Mission:** Software engineering best practices (version control, testing, modularity, documentation) for in-warehouse/lakehouse SQL transformations.
- **Data Guide:** [[dbt/02_Data_Guide|Models, Incremental Strategies & Model Contracts]]
- **Architecture:** [[dbt/03_Architecture|Jinja Compiler, DAG Resolution & Semantic Layer]]
- **Performance:** [[dbt/04_Performance_Guide|Lookback Filters, Slim CI & Microbatching]]

### 2. 🟣 [[Trino/01_Overview|Trino]] (Interactive Data Lake Query Engine)
- **Primary Mission:** Massively Parallel Processing (MPP) query engine for fast interactive SQL queries and cross-source data federation over petabyte-scale lakes.
- **Data Guide:** [[Trino/02_Data_Guide|Connector SPI, Pages & Vectorized Blocks]]
- **Architecture:** [[Trino/03_Architecture|Coordinator/Workers, CBO & In-Memory Exchanges]]
- **Performance:** [[Trino/04_Performance_Guide|Memory Pools, File Sizing (512MB) & Resource Groups]]

### 3. 🔵 [[StarRocks/01_Overview|StarRocks]] (Sub-Second Real-Time OLAP)
- **Primary Mission:** Next-generation C++ SIMD vectorized MPP database for high-concurrency dashboards, real-time CDC upserts, and accelerated data lake querying.
- **Data Guide:** [[StarRocks/02_Data_Guide|Primary Key Engine, Tablets & External Catalogs]]
- **Architecture:** [[StarRocks/03_Architecture|FE/BE Architecture, Pipeline Engine & Local NVMe Cache]]
- **Performance:** [[StarRocks/04_Performance_Guide|Tablet Bucketing, Persistent Index & Async MVs]]

---

## ⚖️ Cross-Engine Comparison
- Read the definitive architectural breakdown: **[[Comparisons/Trino_vs_StarRocks|Trino vs. StarRocks]]**

---

## 🔗 Key Conceptual Connections
- How vectorized SIMD execution accelerates analytical queries: [[Concepts/Vectorized_SIMD_Execution|Vectorized SIMD Execution]]
- Cost-Based Optimization across multi-table joins: [[Concepts/Cost_Based_Optimizer_CBO|Cost-Based Optimizer (CBO)]]
- How dbt executes transformations across storage layers: [[Concepts/Decoupled_Compute_and_Storage|Decoupled Compute & Storage]]
- Real-time CDC synchronization into OLAP tables: [[Concepts/Change_Data_Capture_CDC|Change Data Capture (CDC)]]

---

## 🔍 Underlying Storage Targets
- [[Apache Iceberg/01_Overview|Apache Iceberg]] · [[Apache Paimon/01_Overview|Apache Paimon]] · [[Apache Hive/01_Overview|Apache Hive]]
