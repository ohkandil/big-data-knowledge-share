---
title: "StarRocks: Overview & Foundational Concepts"
type: overview
tags:
  - starrocks
  - olap
  - vectorized
  - real-time-analytics
  - materialized-views
  - simd
  - 01-overview
aliases:
  - "StarRocks"
  - "StarRocks Overview"
layer: "Real-Time OLAP Engine"
parent: "[[MOCs/MOC_Transformation_and_OLAP_Serving]]"
---

# StarRocks: Architectural Overview & Foundational Concepts

```
   ███████╗████████╗ █████╗ ██████╗ ██████╗  ██████╗  ██████╗██╗  ██╗███████╗
   ██╔════╝╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗██╔═══██╗██╔════╝██║ ██╔╝██╔════╝
   ███████╗   ██║   ███████║██████╔╝██████╔╝██║   ██║██║     █████╔╝ ███████╗
   ╚════██║   ██║   ██╔══██║██╔══██╗██╔══██╗██║   ██║██║     ██╔═██╗ ╚════██║
   ███████║   ██║   ██║  ██║██║  ██║██║  ██║╚██████╔╝╚██████╗██║  ██╗███████║
   ╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝  ╚═════╝╚═╝  ╚═╝╚══════╝
   Next-Generation Sub-Second MPP Real-Time OLAP Engine
```

---

## Table of Contents
1. [Executive Summary & Core Mission](#1-executive-summary--core-mission)
2. [Historical Context & Genesis: Why StarRocks Was Created](#2-historical-context--genesis-why-starrocks-was-created)
   - [The Limits of Apache Doris & ClickHouse](#the-limits-of-apache-doris--clickhouse)
   - [Full Vectorized C++ Engine & SIMD Architecture](#full-vectorized-c-engine--simd-architecture)
   - [Unified Real-Time OLAP & Data Lakehouse Analytics](#unified-real-time-olap--data-lakehouse-analytics)
3. [The Modern Streamhouse / Lakehouse Ecosystem Context](#3-the-modern-streamhouse--lakehouse-ecosystem-context)
   - [Positioning StarRocks in the Modern Stack](#positioning-starrocks-in-the-modern-stack)
   - [StarRocks with Flink, Kafka, Iceberg, Paimon, and Hive](#starrocks-with-flink-kafka-iceberg-paimon-and-hive)
4. [High-Level Architectural Topology](#4-high-level-architectural-topology)
   - [Frontend (FE) and Backend (BE) Roles](#frontend-fe-and-backend-be-roles)
   - [Shared-Nothing vs. Shared-Data (Cloud-Native) Architecture](#shared-nothing-vs-shared-data-cloud-native-architecture)
5. [How StarRocks Processes Data at a High Level](#5-how-starrocks-processes-data-at-a-high-level)
   - [Vectorized Execution Engine & SIMD Instruction Pipelines](#vectorized-execution-engine--simd-instruction-pipelines)
   - [Real-Time Primary Key Upsert Engine](#real-time-primary-key-upsert-engine)
   - [Asynchronous Materialized Views with Automated Query Rewrite](#asynchronous-materialized-views-with-automated-query-rewrite)
6. [StarRocks vs. Alternative OLAP Engines](#6-starrocks-vs-alternative-olap-engines)
7. [Key Terminology & Mental Model Glossary](#7-key-terminology--mental-model-glossary)
8. [References & Further Reading](#8-references--further-reading)

---

## 1. Executive Summary & Core Mission

**StarRocks** is an open-source, next-generation Massively Parallel Processing (MPP) analytical database engine designed for **sub-second real-time multi-dimensional analytics, high-concurrency BI dashboards, and direct data lake querying**.

Unlike traditional batch engines (Spark, Hive) or general-purpose federated query engines (Trino) that incur high per-query overhead, StarRocks was engineered from scratch in **C++ with full SIMD vectorization** to deliver 3x–10x higher performance on complex analytical queries involving multi-table joins, aggregations, and high-frequency real-time upserts.

### Primary Capabilities:
- **Blazing Fast Vectorized C++ Engine:** Every operator, expression, and data structure is optimized for modern CPU SIMD instruction sets (AVX-512, AVX2).
- **Sub-Second Multi-Table Joins:** Sophisticated Cost-Based Optimizer (CBO) capable of executing distributed hash joins and runtime filtering across dozens of tables in milliseconds without pre-computing wide denormalized tables.
- **Real-Time Primary Key Upserts:** High-throughput streaming writes and record updates (up to millions of upserts/sec) with real-time consistency for CDC streams from databases.
- **Unified Lakehouse Querying:** Zero-copy external catalog integration querying Apache Iceberg, Apache Paimon, Apache Hudi, and Hive directly on S3/ADLS/HDFS at near-native speeds.
- **Intelligent Materialized Views:** Multi-table asynchronous Materialized Views with automated transparent query rewrite and incremental refresh.

```
   [ Real-Time Stream CDC ]               [ Data Lakehouse Storage ]
   (Kafka / Flink / Flink CDC)            (S3 / ADLS: Iceberg / Paimon)
               │                                      │
               ▼                                      ▼
   ┌──────────────────────────────────────────────────────────────────────────┐
   │                           STARROCKS CLUSTER                              │
   │  ┌─────────────────────────────────┐ ┌────────────────────────────────┐ │
   │  │ Frontend (FE): Java Planner/CBO │ │ Backend (BE): C++ Vectorized   │ │
   │  │ (Catalog, Parser, Scheduler)    │ │ (SIMD Operators, Primary Key)  │ │
   │  └─────────────────────────────────┘ └────────────────────────────────┘ │
   └────────────────────────────────────┬─────────────────────────────────────┘
                                        │ Sub-second OLAP Query Results (<100ms)
                                        ▼
   [ BUSINESS DASHBOARDS & SERVING APIS ]
   • Customer-Facing Embedded Analytics
   • High-Concurrency BI Dashboards (Tableau, Superset)
   • Real-Time Alerting & Metric Engines
```

---

## 2. Historical Context & Genesis: Why StarRocks Was Created

### The Limits of Apache Doris & ClickHouse

In the late 2010s, modern data teams faced clear architectural limitations with existing OLAP engines:
1. **ClickHouse:** Extremely fast for single-table aggregations (wide flat tables / "One Big Table" paradigm). However, ClickHouse struggled severely with **distributed multi-table joins** and lacked a mature Cost-Based Optimizer. Engineering teams spent hundreds of hours maintaining fragile ETL pipelines just to denormalize data into giant single tables.
2. **Apache Doris (Pre-2020):** Strong support for relational modeling and MySQL protocol compatibility, but its Java/C++ hybrid execution engine lacked modern vectorized execution, leading to high CPU overhead and slow query latency on complex multi-join queries.

### Full Vectorized C++ Engine & SIMD Architecture

In 2020, the core engineering team behind Doris forked the project to create **StarRocks** (initially Doris-On-Steroids / StarRocks Inc., later donated to the Linux Foundation). 

They completely threw out the legacy Volcano execution engine and rewrote the backend in **pure C++ with full-columnar vectorization**:
- Replaced tuple-at-a-time processing with columnar batch chunk processing.
- Leveraged CPU SIMD (Single Instruction, Multiple Data) vector instructions to process multiple data elements per CPU clock cycle.
- Built an enterprise-grade **Cascades-style Cost-Based Optimizer (CBO)** designed specifically to optimize complex multi-table joins on star and snowflake schemas.

---

## 3. The Modern Streamhouse / Lakehouse Ecosystem Context

### Positioning StarRocks in the Modern Stack

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                             MODERN STREAMHOUSE TOPOLOGY                                  │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│  CONTINUOUS STREAMING & CDC INGESTION                                                    │
│  Kafka / Flink / Debezium ──► [ Direct Routine Load / Stream Load ]                       │
│                                           │                                              │
│                                           ▼                                              │
│  REAL-TIME OLAP SERVING & DATA LAKEHOUSE ACCELERATOR                                     │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                  STARROCKS                                         │  │
│  │  • Real-Time Internal Tables: Primary Key Upserts (Sub-second CDC sync)            │  │
│  │  • External Lakehouse Catalogs: Native Iceberg, Paimon, Hudi, Hive readers        │  │
│  │  • Asynchronous Materialized Views: Automated query rewrite across lake & internal │  │
│  │  • Data Cache: Local NVMe caching for remote cloud object storage (S3/ADLS)        │  │
│  └────────────────────────────────────┬───────────────────────────────────────────────┘  │
│                                       │                                                  │
│                                       ▼ Sub-second SQL Latency (<50ms)                   │
│  [ HIGH-CONCURRENCY CONSUMERS ]                                                          │
│  • Customer-Facing Embedded Analytics (10,000+ QPS)                                      │
│  • Interactive BI Dashboards & Ad-hoc Exploration                                        │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. High-Level Architectural Topology

StarRocks features a simplified two-tier architecture: **Frontend (FE)** and **Backend (BE)**.

```
                             ┌───────────────────────────────┐
                             │    CLIENT / BI / MYSQL CLI    │
                             │   (Standard MySQL Protocol)   │
                             └───────────────┬───────────────┘
                                             │
                                             ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                 FRONTEND (FE) CLUSTER                                   │
│  ┌───────────────────────────────────┐        ┌──────────────────────────────────────┐  │
│  │ FE Leader (Java)                  │        │ FE Follower / Observer (Java)        │  │
│  │  • MySQL Protocol Handler & Auth  │◄──────►│  • Read-Only Query Parsing & Plan    │  │
│  │  • Metadata Store (BDB-JE Raft)   │  Meta  │  • High-Concurrency Query Scale-Out  │  │
│  │  • Cascades Cost-Based Optimizer  │  Sync  └──────────────────────────────────────┘  │
│  └───────────────────────────────────┘                                                  │
└────────────────────────────────────────┬────────────────────────────────────────────────┘
                                         │ Distributed Execution Fragment Plan
        ┌────────────────────────────────┼────────────────────────────────┐
        ▼                                ▼                                ▼
┌──────────────────────────────┐ ┌──────────────────────────────┐ ┌───────────────────────┐
│       BACKEND (BE) 1         │ │       BACKEND (BE) 2         │ │     BACKEND (BE) 3    │
│  ┌────────────────────────┐  │ │  ┌────────────────────────┐  │ │ ┌───────────────────┐ │
│  │ Vectorized C++ Engine  │  │ │  │ Vectorized C++ Engine  │  │ │ │ Vectorized Engine │ │
│  │ (SIMD AVX-512 Operators│  │ │  │ (SIMD AVX-512 Operators│  │ │ │ (SIMD Operators)  │ │
│  └────────────────────────┘  │ │  └────────────────────────┘  │ │ └───────────────────┘ │
│  ┌────────────────────────┐  │ │  ┌────────────────────────┐  │ │ ┌───────────────────┐ │
│  │ Storage Engine:        │  │ │  │ Storage Engine:        │  │ │ │ Storage Engine:   │ │
│  │ Primary Key / Tablets  │  │ │  │ Primary Key / Tablets  │  │ │ │ Tablets / Cache   │ │
│  └────────────────────────┘  │ │  └────────────────────────┘  │ │ └───────────────────┘ │
│  ┌────────────────────────┐  │ │  ┌────────────────────────┐  │ │ ┌───────────────────┐ │
│  │ Local NVMe Data Cache  │  │ │  │ Local NVMe Data Cache  │  │ │ │ Data Cache        │ │
│  └────────────────────────┘  │ │  └────────────────────────┘  │ │ └───────────────────┘ │
└──────────────────────────────┘ └──────────────────────────────┘ └───────────────────────┘
```

### Components:
1. **Frontend (FE):**
   - Written in Java. Implements MySQL wire protocol (connect with standard MySQL drivers/BI tools).
   - Manages cluster metadata, catalog definitions, user permissions, table schemas, and partition layouts using an embedded BDB-JE Raft consensus log.
   - Runs the **Cascades Cost-Based Optimizer (CBO)** to generate distributed execution plans.
2. **Backend (BE):**
   - Written in pure C++.
   - Executes query plan fragments using vectorized operators.
   - Manages physical data storage in partitioned **Tablets** (LSM/columnar format) and handles high-speed streaming writes.
   - Houses the local **Data Cache** for caching S3/Iceberg blocks on local NVMe SSDs.

---

## 5. How StarRocks Processes Data at a High Level

### Vectorized Execution Engine & SIMD Instruction Pipelines

Traditional database engines (Volcano model) process data row-by-row via virtual function calls:
```cpp
// Legacy Row-at-a-time (High instruction cache misses, no SIMD)
for (row : rows) {
    if (eval_filter(row)) {
        project(row);
    }
}
```

StarRocks processes data in **Columnar Chunks** of thousands of rows:
```cpp
// Vectorized SIMD Loop (AVX-512 evaluates 16 values per clock cycle)
void evaluate_filter_vectorized(int32_t* col, uint8_t* bitmask, int n) {
    #pragma clang loop vectorize(enable)
    for (int i = 0; i < n; i++) {
        bitmask[i] = (col[i] > 100);
    }
}
```

### Real-Time Primary Key Upsert Engine

StarRocks features a dedicated **Primary Key Table Engine**:
- Uses an in-memory **Delete Bitmap** and RocksDB-backed key index.
- When an update arrives, StarRocks marks the old row position in the delete bitmap and appends the new record sequentially.
- Eliminates expensive merge-on-read overhead during query time, enabling sub-second queries even during massive concurrent streaming CDC ingestion.

### Asynchronous Materialized Views with Automated Query Rewrite

- Users can create multi-table Materialized Views over internal tables or external Iceberg/Paimon tables on S3.
- When an incoming ad-hoc SQL query arrives, StarRocks's CBO **automatically rewrites the query** to read from the pre-aggregated Materialized View without requiring users to change their SQL code.

---

## 6. StarRocks vs. Alternative OLAP Engines

| Dimension | StarRocks | ClickHouse | Trino | Apache Doris |
| :--- | :--- | :--- | :--- | :--- |
| **Engine Language** | C++ (Full SIMD Vectorization) | C++ (Vectorized) | Java (JVM) | C++ / Java hybrid |
| **Multi-Table Join Performance** | Exceptional (Cascades CBO + Runtime Filter) | Poor (Requires manual denormalization) | Excellent (In-memory distributed hash joins) | Good (CBO) |
| **Real-Time CDC Upserts** | Primary Key Engine (Delete Bitmap) | ReplacingMergeTree (Eventual consistency) | Limited (Storage-dependent) | Unique Key Engine (Merge-On-Read / COW) |
| **Data Lakehouse Querying** | Direct Lakehouse reading + NVMe Cache | S3/HDFS Table Engines (limited) | Core Strength (30+ Connectors) | External Catalogs |
| **Materialized Views** | Multi-table with automatic query rewrite | Single-table trigger-based views | Manual refresh tables | Multi-table Materialized Views |
| **Standard Protocol** | MySQL Wire Protocol (port 9030) | Native ClickHouse / HTTP / MySQL | Custom REST / Trino JDBC | MySQL Wire Protocol |

---

## 7. Key Terminology & Mental Model Glossary

- **Frontend (FE):** Master node responsible for metadata management, connection handling, and query planning.
- **Backend (BE):** Worker node executing vectorized query operators and managing data tablets.
- **Tablet:** The fundamental data storage shard in StarRocks (partitioned and bucketed).
- **Primary Key Model:** Table type optimized for high-frequency CDC upserts using delete bitmaps.
- **Duplicate Key Model:** Table type optimized for append-only raw event/log data.
- **Aggregate Key Model:** Table type pre-aggregating metrics (SUM, MIN, MAX) on identical dimension keys.
- **Data Cache:** Local NVMe disk cache accelerating queries on external lakehouse tables (Iceberg/Paimon on S3).
- **Pipeline Engine:** StarRocks's thread-pool execution model replacing thread-per-query with non-blocking coroutine-like task workers.

---

## 8. References & Further Reading

1. **Official StarRocks Documentation:** [https://docs.starrocks.io/](https://docs.starrocks.io/)
2. **StarRocks Architecture Overview:** [https://docs.starrocks.io/docs/architecture/architecture/](https://docs.starrocks.io/docs/architecture/architecture/)
3. **StarRocks GitHub Repository:** [https://github.com/StarRocks/starrocks](https://github.com/StarRocks/starrocks)
4. **Vectorized Query Engine Design:** [https://docs.starrocks.io/docs/architecture/vectorized-engine/](https://docs.starrocks.io/docs/architecture/vectorized-engine/)


---

**Layer:** ⭐ Real-Time OLAP Engine  
**Parent MOC:** [[MOCs/MOC_Transformation_and_OLAP_Serving|MOC: Transformation & OLAP Serving]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** → [[StarRocks/02_Data_Guide|Data Guide]]

**Related technologies:** [[Trino/01_Overview|Trino]] · [[dbt/01_Overview|dbt]] · [[Apache Iceberg/01_Overview|Apache Iceberg]] · [[Apache Paimon/01_Overview|Apache Paimon]] · [[Apache Kafka/01_Overview|Apache Kafka]]

**Core concepts:** [[Concepts/LSM_Trees_and_Compaction|LSM-Trees & Compaction]]
