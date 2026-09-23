# Architectural Comparison: Trino vs. StarRocks

```
   ┌────────────────────────────────────────────────────────────────────────┐
   │                         TRINO vs. STARROCKS                            │
   │            Interactive Data Lake Federation vs. Real-Time OLAP         │
   └────────────────────────────────────────────────────────────────────────┘
```

---

## Table of Contents
1. [Executive Summary & Core Philosophy](#1-executive-summary--core-philosophy)
2. [Architectural Deep-Dive Comparison](#2-architectural-deep-dive-comparison)
3. [Vectorized Execution & Computational Mechanics](#3-vectorized-execution--computational-mechanics)
4. [Storage Engine & Lakehouse Querying Capabilities](#4-storage-engine--lakehouse-querying-capabilities)
5. [Real-Time CDC Ingestion & Upsert Handling](#5-real-time-cdc-ingestion--upsert-handling)
6. [Multi-Table Join Performance & Optimizer Architecture](#6-multi-table-join-performance--optimizer-architecture)
7. [Materialized Views & Caching Strategies](#7-materialized-views--caching-strategies)
8. [Comprehensive Feature Comparison Matrix](#8-comprehensive-feature-comparison-matrix)
9. [Architecture Decision Framework (When to Choose Which)](#9-architecture-decision-framework-when-to-choose-which)
10. [References & Further Reading](#10-references--further-reading)

---

## 1. Executive Summary & Core Philosophy

| Engine | Core Philosophy | Primary Identity |
| :--- | :--- | :--- |
| **Trino** | *"SQL-on-Everything"* — Pure compute layer that decouples query processing from storage. Connects to 30+ disparate data sources without moving data. | **Interactive Data Lakehouse Query Engine & Cross-Source Federation Layer** |
| **StarRocks** | *"Blazing Fast Vectorized Analytics"* — Next-generation MPP database providing sub-second OLAP queries across both internal real-time tables and external open data lakes. | **High-Concurrency Real-Time OLAP & Lakehouse Acceleration Engine** |

---

## 2. Architectural Deep-Dive Comparison

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   TRINO ARCHITECTURE                                    │
│                                                                                         │
│     [ Client / BI ] ──► [ Coordinator (Java) ] ──► Schedules Splits                     │
│                                │                                                        │
│             ┌──────────────────┴──────────────────┐                                     │
│             ▼                                     ▼                                     │
│     [ Worker 1 (Java) ] ◄──In-Memory Exchange──► [ Worker 2 (Java) ]                   │
│             │                                     │                                     │
│             ▼ (Pipelined SPI Stream)              ▼ (Pipelined SPI Stream)              │
│    [ External Storage: S3 / Iceberg / Hive / Postgres / Kafka / Elasticsearch ]         │
└─────────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                 STARROCKS ARCHITECTURE                                  │
│                                                                                         │
│     [ Client / MySQL ] ──► [ Frontend (Java / BDB-JE Raft Quorum) ]                     │
│                                │                                                        │
│             ┌──────────────────┴──────────────────┐                                     │
│             ▼                                     ▼                                     │
│     [ Backend 1 (C++ SIMD) ] ◄──Data Shuffle───► [ Backend 2 (C++ SIMD) ]               │
│     • Vectorized Pipeline Engine                 • Vectorized Pipeline Engine           │
│     • Primary Key Table Storage                  • Primary Key Table Storage            │
│     • Local NVMe Data Cache                      • Local NVMe Data Cache                │
│             │                                     │                                     │
│             ▼ (Zero-Copy Native Reader)           ▼ (Zero-Copy Native Reader)           │
│    [ External Lakehouse: Iceberg / Paimon / Hive on S3 ] + [ Internal OLAP Storage ]    │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Vectorized Execution & Computational Mechanics

### Trino: JVM Vectorization & Bytecode Compilation
- Written in Java.
- Generates dynamic JVM bytecode at runtime using ASM to inline expressions and eliminate virtual method calls.
- Employs columnar **Pages** and **Blocks** in memory, relying on the JVM JIT compiler to generate SIMD instructions.

### StarRocks: Native C++ SIMD Vectorization
- Written from the ground up in native **C++20**.
- Explicitly compiled for modern CPU vector extensions (**AVX-512, AVX2, ARM Neon**).
- Columnar Chunk data structures are memory-aligned to CPU cache line boundaries, evaluating operations (filters, arithmetic, hashing) on 16 to 32 data values per single CPU clock cycle.
- **Performance Impact:** StarRocks typically exhibits **2x to 5x higher raw CPU compute throughput** on heavy compute/filter/aggregation workloads compared to JVM-based engines.

---

## 4. Storage Engine & Lakehouse Querying Capabilities

### Data Lakehouse (Iceberg/Paimon/Hive) Querying:
- **Trino:** The benchmark standard for data lake exploration. Comprehensive metadata caching, file footer caching, and mature SPI connectors supporting complex table formats and time-travel syntax (`FOR TIMESTAMP AS OF`).
- **StarRocks:** Features native C++ readers for Iceberg, Paimon, Hudi, and Hive tables. Utilizes a **Local NVMe Data Cache** that automatically caches remote S3 Parquet blocks locally, achieving sub-second latency on data lake queries comparable to internal database tables.

---

## 5. Real-Time CDC Ingestion & Upsert Handling

| Capability | Trino | StarRocks |
| :--- | :--- | :--- |
| **High-Frequency Writes** | Not designed for direct streaming inserts. Relies on external engines (Flink/Spark) committing to Iceberg. | Built-in streaming ingestion via **Stream Load** and **Routine Load** (Kafka). |
| **CDC Upserts / Deletes** | Indirect (writes equality/positional deletes to Iceberg table format). | Native **Primary Key Engine** with in-memory **Delete Bitmaps** handling millions of real-time upserts/sec. |
| **Point-in-Time Freshness** | Bounded by lakehouse checkpoint commit intervals (30s–60s). | Real-time / Sub-second (<100ms) data freshness from Kafka. |

---

## 6. Multi-Table Join Performance & Optimizer Architecture

- **Trino:** Features an advanced Cost-Based Optimizer (CBO) supporting automated Broadcast vs. Partitioned joins and runtime dynamic filtering. Highly effective on wide snowflake/star schemas across distributed workers.
- **StarRocks:** Implements a **Cascades-style Cost-Based Optimizer** combined with native C++ hash tables and **global runtime Bloom filters**. Because join probe loops are vectorized with SIMD, multi-table joins execute with extreme speed without requiring pre-computed denormalized tables.

---

## 7. Materialized Views & Caching Strategies

- **Trino:** Supports basic Materialized Views on connectors like Iceberg, but queries must explicitly target the Materialized View name (no automatic query rewrite).
- **StarRocks:** Industry-leading **Asynchronous Multi-Table Materialized Views**. The CBO automatically analyzes ad-hoc queries targeting raw tables and **transparently rewrites the execution plan** to scan pre-aggregated Materialized Views instead, dropping query latency from seconds to milliseconds without modifying user SQL code.

---

## 8. Comprehensive Feature Comparison Matrix

| Feature / Dimension | Trino | StarRocks |
| :--- | :--- | :--- |
| **Primary Architecture** | Pure Compute (Federated MPP) | Hybrid (Internal MPP Storage + External Lakehouse Querying) |
| **Implementation Language** | Java (JVM) | C++ (Backend) / Java (Frontend) |
| **Client Protocol** | Custom REST / Trino JDBC / ODBC | MySQL Wire Protocol (port 9030) |
| **Target Latency Profile** | 100ms – 30s (Interactive / Ad-hoc) | 10ms – 2s (Real-Time OLAP / Sub-Second Dashboards) |
| **Query Federation (30+ Sources)**| **Superior** (Postgres, S3, Kafka, Mongo, ES, Redis) | Moderate (Iceberg, Paimon, Hudi, Hive, JDBC) |
| **Concurrency Capacity (QPS)** | Moderate (10–100 concurrent queries) | **High** (1,000–10,000+ concurrent queries via Observers) |
| **Internal Storage Option** | No (External storage only) | Yes (Primary Key, Duplicate, Aggregate models) |
| **Real-Time CDC Stream Writes** | No (Requires external engine) | **Yes** (Native Routine Load / Stream Load) |
| **Automated Query Rewrite for MVs**| No | **Yes** (Cascades CBO automated rewrite) |
| **Local Lakehouse Cache** | RubiX / FileSystem Cache (Configurable) | **Native NVMe Data Cache** (Built-in) |
| **Fault-Tolerant Long Batch ETL** | **Yes** (Project Tardigrade / Exchange Spooling) | Pipeline engine (Optimized for fast-fail execution) |

---

## 9. Architecture Decision Framework (When to Choose Which)

```
                                [ WHAT IS YOUR PRIMARY WORKLOAD? ]
                                                │
                 ┌──────────────────────────────┴──────────────────────────────┐
                 ▼                                                             ▼
     [ AD-HOC DATA LAKE EXPLORATION & ]                           [ HIGH-CONCURRENCY REAL-TIME OLAP & ]
     [ HETEROGENEOUS FEDERATION       ]                           [ CUSTOMER-FACING DASHBOARDS        ]
                 │                                                             │
                 ├─ Querying across 5+ different databases                     ├─ Sub-second dashboard SLA (<100ms)
                 │  (e.g., S3 + PostgreSQL + Elasticsearch)                    ├─ High QPS (Hundreds/Thousands of users)
                 ├─ Ad-hoc exploration on massive multi-PB lakes               ├─ Real-time streaming CDC sync from Kafka
                 ├─ Batch transformation pipelines (dbt-trino)                 ├─ Multi-table join acceleration via MVs
                 │                                                             │
                 ▼                                                             ▼
         CHOOSE: **TRINO**                                            CHOOSE: **STARROCKS**
```

### Modern Enterprise Coexistence Pattern:
In leading lakehouse architectures, **Trino and StarRocks operate together**:
1. **Trino** serves internal data scientists, analysts, and dbt batch transformation jobs querying the entire enterprise data lake.
2. **StarRocks** powers customer-facing embedded analytics, real-time executive dashboards, and sub-second metrics serving directly from Kafka and Iceberg.

---

## 10. References & Further Reading

1. **Trino Documentation:** [https://trino.io/docs/current/](https://trino.io/docs/current/)
2. **StarRocks Documentation:** [https://docs.starrocks.io/](https://docs.starrocks.io/)
3. **Presto: SQL on Everything (ACM SIGMOD Paper):** [https://dl.acm.org/doi/10.1145/3299869.3314053](https://dl.acm.org/doi/10.1145/3299869.3314053)
4. **StarRocks Architecture Whitepaper:** [https://www.starrocks.io/](https://www.starrocks.io/)
