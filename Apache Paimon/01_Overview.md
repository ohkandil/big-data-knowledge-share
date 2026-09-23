# Apache Paimon: Architectural Overview & Foundational Concepts

```
   ██████╗  █████╗ ██╗███╗   ███╗ ██████╗ ███╗   ██╗
   ██╔══██╗██╔══██╗██║████╗ ████║██╔═══██╗████╗  ██║
   ██████╔╝███████║██║██╔████╔██║██║   ██║██╔██╗ ██║
   ██╔═══╝ ██╔══██║██║██║╚██╔╝██║██║   ██║██║╚██╗██║
   ██║     ██║  ██║██║██║ ╚═╝ ██║╚██████╔╝██║ ╚████║
   ╚═╝     ╚═╝  ╚═╝╚═╝╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
   Streaming-First Lakehouse Data Lake Storage Format
```

---

## Table of Contents
1. [Executive Summary & Core Mission](#1-executive-summary--core-mission)
2. [Historical Context & Genesis: Why Paimon Was Created](#2-historical-context--genesis-why-paimon-was-created)
   - [The Limits of Apache Iceberg & Delta Lake for Streaming](#the-limits-of-apache-iceberg--delta-lake-for-streaming)
   - [Evolution from Flink Table Store](#evolution-from-flink-table-store)
   - [LSM-Tree on Cloud Object Storage](#lsm-tree-on-cloud-object-storage)
3. [The Modern Streamhouse / Lakehouse Ecosystem Context](#3-the-modern-streamhouse--lakehouse-ecosystem-context)
   - [Positioning Paimon in the Streamhouse Stack](#positioning-paimon-in-the-streamhouse-stack)
   - [Paimon with Flink, Spark, Trino, and StarRocks](#paimon-with-flink-spark-trino-and-starrocks)
4. [High-Level Architectural Topology](#4-high-level-architectural-topology)
   - [Primary Key Tables vs. Append-Only Tables](#primary-key-tables-vs-append-only-tables)
   - [The LSM File Hierarchy on Storage](#the-lsm-file-hierarchy-on-storage)
5. [How Paimon Processes Data at a High Level](#5-how-paimon-processes-data-at-a-high-level)
   - [Streaming Ingestion & Sub-Minute Checkpoint Commits](#streaming-ingestion--sub-minute-checkpoint-commits)
   - [Changelog Generation for Downstream Streaming Consumers](#changelog-generation-for-downstream-streaming-consumers)
   - [Deletion Vectors & Compaction Engine](#deletion-vectors--compaction-engine)
6. [Paimon vs. Alternative Lakehouse Formats](#6-paimon-vs-alternative-lakehouse-formats)
7. [Key Terminology & Mental Model Glossary](#7-key-terminology--mental-model-glossary)
8. [References & Further Reading](#8-references--further-reading)

---

## 1. Executive Summary & Core Mission

**Apache Paimon** (formerly **Flink Table Store**) is an open-source, **streaming-first lakehouse table format** specifically designed to bridge the gap between high-throughput continuous stream processing and high-performance batch/OLAP analytical querying on cloud object storage (Amazon S3, Azure ADLS, Google Cloud Storage, HDFS).

While traditional lakehouse table formats (like Apache Iceberg and Delta Lake) originated from batch-first architectures where data is written in large batches every few minutes or hours, Apache Paimon brings the **Log-Structured Merge-tree (LSM-tree)** storage paradigm directly to cloud object storage. This enables:
- High-frequency, sub-minute streaming upserts and deletes.
- High-throughput point lookups and primary-key aggregations.
- Native **Changelog Generation** directly from storage, allowing downstream Flink streaming jobs to consume row-level change events ($+I, -U, +U, -D$) without maintaining expensive message queues like Apache Kafka for intermediate pipeline stages.

### Primary Capabilities:
- **Streaming-First LSM Storage Engine:** High-frequency, low-latency streaming writes without generating massive small file fragmentation or metadata write bottlenecks.
- **High-Performance Real-Time CDC Upserts:** Native primary-key deduplication and partial-column updates directly on S3/HDFS.
- **Native Streaming Changelog Producer:** Automatically derives and emits row-level change data streams (`INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE`) for downstream streaming consumers.
- **Fast Interactive OLAP Reads:** Fully integrated with Trino, StarRocks, Apache Spark, and Flink SQL for vectorized columnar scanning.
- **Dynamic Bucketing & Auto-Compaction:** Automatically scales buckets dynamically with data volume growth.

```
   [ Streaming CDC / Logs ]
   (Kafka / Flink CDC)
           │
           ▼ Continuous Sub-Minute Streaming Commits
   ┌────────────────────────────────────────────────────────────────────────┐
   │                          APACHE PAIMON TABLE                           │
   │  ┌──────────────────────────────────────────────────────────────────┐  │
   │  │ LSM-Tree Storage on S3 / ADLS (Parquet / ORC SST Data Files)     │  │
   │  │ • Level 0 (MemTable Flush) ──► Level 1 ──► Level 2 (Compacted)   │  │
   │  └──────────────────────────────────────────────────────────────────┘  │
   └───────────────┬────────────────────────────────────────┬───────────────┘
                   │                                        │
                   ▼ Streaming Changelog Feed               ▼ Vectorized Fast OLAP Scans
   [ Downstream Apache Flink Pipeline ]       [ StarRocks / Trino / Spark SQL ]
   (Continuous Real-Time Aggregations)        (Interactive Business Dashboards)
```

---

## 2. Historical Context & Genesis: Why Paimon Was Created

### The Limits of Apache Iceberg & Delta Lake for Streaming

Between 2018 and 2022, Apache Iceberg and Delta Lake transformed big data by bringing ACID transactions to S3. However, data engineering teams operating high-velocity real-time streaming architectures encountered severe pain points:
1. **The Small File & Compaction Bottleneck:** Ingestion pipelines committing data every 10–30 seconds generated tens of thousands of tiny Parquet files and equality delete files, degrading read performance and causing heavy metadata lock contention during commits.
2. **Expensive Upserts on Object Stores:** Updating a single row in traditional formats often required rewriting entire 128MB Parquet files (Copy-on-Write) or writing positional delete files (Merge-on-Read), which introduces extreme CPU and memory overhead during query scans.
3. **The "Dual Pipeline" Kafka Dilemma:** To feed downstream streaming consumers, companies had to keep data in Apache Kafka topics indefinitely (expensive storage) because data lake tables could not reliably produce a clean changelog stream.

### Evolution from Flink Table Store

In 2022, the Apache Flink core team introduced **Flink Table Store** to provide a native, streaming-optimized storage layer. In March 2023, the project was accepted into the Apache Software Foundation Incubator and rebranded as **Apache Paimon**, graduating as a Top-Level Apache Project in 2024.

Paimon adopted the **LSM-tree architecture** (used by RocksDB and ClickHouse) and adapted it to cloud object storage:
- Writes are buffered into memory (`MemTable`) and flushed as immutable Sorted String Table (SST) files.
- Background compaction asynchronously merges and sorts files across levels.
- Primary key lookups, updates, and deletes operate with logarithmic time complexity.

---

## 3. The Modern Streamhouse / Lakehouse Ecosystem Context

### Positioning Paimon in the Streamhouse Stack

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                             MODERN STREAMHOUSE TOPOLOGY                                  │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│  TRANSACTIONAL SOURCES                                                                   │
│  PostgreSQL / MySQL / Oracle WAL ──► [ Flink CDC ]                                       │
│                                           │                                              │
│                                           ▼ Direct Real-Time Streaming Ingestion         │
│  STREAMHOUSE STORAGE LAYER                                                               │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                  APACHE PAIMON                                     │  │
│  │  • Real-Time Primary Key Upsert Tables (CDC Synced)                                │  │
│  │  • Append-Only Log Tables (Clickstreams / IoT Telemetry)                           │  │
│  │  • Automatic Asynchronous Compaction & Dynamic Bucketing                           │  │
│  │  • Changelog Producer: Serves as the Real-Time Storage Bus (Replaces Kafka Tier)   │  │
│  └────────────────────────────────────┬───────────────────────────────────────────────┘  │
│                                       │                                                  │
│         ┌─────────────────────────────┴─────────────────────────────┐                    │
│         ▼ (Streaming Changelog Reads)                               ▼ (Fast Vectorized)  │
│  [ CONTINUOUS STREAM COMPUTE ]                               [ REAL-TIME OLAP SERVING ]  │
│  Apache Flink Streaming Jobs                                 StarRocks / Trino / Spark   │
│  (Cascading Real-Time Aggregations)                          (Sub-second Dashboards)     │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. High-Level Architectural Topology

Paimon provides two fundamental table types:

### 1. Primary Key Tables
- Enforce unique primary keys (e.g., `user_id`, `order_id`).
- Incoming updates replace existing records using the LSM-tree engine.
- Supports **Merge Engines**:
  - `deduplicate`: Keeps the latest record.
  - `partial-update`: Multiple streams update different columns of the same row independently without reading existing data.
  - `aggregation`: Pre-aggregates metrics (sum, min, max, listagg) directly inside storage during compaction.

### 2. Append-Only Tables
- Optimized for immutable event logs, sensor metrics, and audit streams.
- Provides ultra-high-speed sequential write throughput and compact file layouts.

---

## 5. How Paimon Processes Data at a High Level

### Streaming Ingestion & Sub-Minute Checkpoint Commits

1. **Flink TaskManager Ingest:** Flink source operators read CDC logs or Kafka events and route records to Paimon writer operators partitioned by bucket.
2. **MemTable Buffering:** Writers buffer records in an in-memory `MemTable` sorted by primary key.
3. **Flushing to Level 0:** When the `MemTable` fills, it flushes a Parquet/ORC SST file directly to S3 under Level 0 (`L0`).
4. **Checkpoint Commits:** Every Flink checkpoint barrier (e.g., every 30s), Paimon commits a new **Snapshot** metadata file using Two-Phase Commit (2PC).

### Changelog Generation for Downstream Streaming Consumers

Paimon can generate exact row-level change events during streaming reads:
- **`input` Changelog Mode:** Emits changes directly from the input stream.
- **`lookup` Changelog Mode:** Performs high-speed local SSD lookups to generate `UPDATE_BEFORE` ($-U$) and `UPDATE_AFTER` ($+U$) events before committing data to storage.
- **`full-compaction` Mode:** Emits changelog deltas during background LSM compactions.

---

## 6. Paimon vs. Alternative Lakehouse Formats

| Dimension | Apache Paimon | Apache Iceberg | Delta Lake | Apache Hudi |
| :--- | :--- | :--- | :--- | :--- |
| **Origin & Design** | Streaming-First (Flink Table Store) | Batch-First / General Lakehouse Standard | Databricks Lakehouse Platform | Streaming Upserts on Hadoop/HDFS |
| **Storage Architecture** | **LSM-Tree on S3** (Multi-level SSTs) | Snapshot Manifest Tree (Data + Delete files) | Transaction Log (`_delta_log`) + Parquet | Timeline + File Groups (COW / MOR) |
| **Streaming CDC Upserts** | **Superior** (High throughput, sub-minute) | Moderate (Positional/Equality deletes) | Moderate (Merge-on-Read) | High (Merge-on-Read) |
| **Changelog Generation** | **Native Built-In** (Replaces Kafka bus) | Limited (CDC table format features) | CDF (Change Data Feed) | Incremental Pull |
| **Partial Column Updates** | **Native** (In-storage field merge) | Requires full row rewrite | Requires full row merge | Supported |
| **Query Engine Support** | Flink, StarRocks, Trino, Spark | Universal (Flink, Trino, Spark, StarRocks, etc.) | Strong (Spark, Trino, StarRocks) | Flink, Spark, Trino |

---

## 7. Key Terminology & Mental Model Glossary

- **Snapshot:** An immutable point-in-time state of the table created on every commit.
- **LSM-Tree:** Log-Structured Merge-tree storage layout organizing data into hierarchical levels ($L_0, L_1, L_2$).
- **Bucket:** The smallest unit of physical data partitioning within a table partition.
- **Dynamic Bucketing:** Paimon's capability to automatically assign and scale bucket counts based on real-time data ingestion volume.
- **Merge Engine:** The algorithm governing how overlapping primary key records are resolved during compaction (`deduplicate`, `partial-update`, `aggregation`).
- **Changelog Producer:** The mechanism responsible for producing row-level before/after mutation events for downstream streaming pipelines.
- **Manifest List / File:** Metadata files tracking active data files across snapshots.

---

## 8. References & Further Reading

1. **Official Apache Paimon Documentation:** [https://paimon.apache.org/](https://paimon.apache.org/)
2. **Paimon GitHub Repository:** [https://github.com/apache/paimon](https://github.com/apache/paimon)
3. **Paimon Streaming CDC Architecture:** [https://paimon.apache.org/docs/master/primary-key-table/cdc-ingestion/](https://paimon.apache.org/docs/master/primary-key-table/cdc-ingestion/)
4. **Paimon LSM Table Design:** [https://paimon.apache.org/docs/master/concepts/basic-concepts/](https://paimon.apache.org/docs/master/concepts/basic-concepts/)
