# Architectural Comparison: Apache Iceberg vs. Apache Paimon vs. Apache Hive

```
   ┌────────────────────────────────────────────────────────────────────────┐
   │                  LAKEHOUSE TABLE FORMATS COMPARISON                    │
   │               Apache Iceberg vs. Apache Paimon vs. Apache Hive         │
   └────────────────────────────────────────────────────────────────────────┘
```

---

## Table of Contents
1. [Executive Summary & Core Architectural Paradigms](#1-executive-summary--core-architectural-paradigms)
2. [Metadata Architecture & Tracking Granularity](#2-metadata-architecture--tracking-granularity)
3. [ACID Guarantees & Concurrency Control](#3-acid-guarantees--concurrency-control)
4. [Streaming Ingestion, Upsert & CDC Performance](#4-streaming-ingestion-upsert--cdc-performance)
5. [Row-Level Deletes & Mutation Mechanics](#5-row-level-deletes--mutation-mechanics)
6. [Partitioning Models: Directory vs. Hidden vs. LSM Bucketing](#6-partitioning-models-directory-vs-hidden-vs-lsm-bucketing)
7. [Schema Evolution & Data Safety](#7-schema-evolution--data-safety)
8. [Multi-Engine Interoperability Ecosystem](#8-multi-engine-interoperability-ecosystem)
9. [Comprehensive Comparison Matrix](#9-comprehensive-comparison-matrix)
10. [Architecture Decision Framework (When to Choose Which)](#10-architecture-decision-framework-when-to-choose-which)
11. [References & Further Reading](#11-references--further-reading)

---

## 1. Executive Summary & Core Architectural Paradigms

| Table Format | Core Identity | Architectural Paradigm | Primary Use Case |
| :--- | :--- | :--- | :--- |
| **Apache Iceberg** | The Universal Open Lakehouse Standard | **Hierarchical Snapshot Tree** (Manifest Lists $\to$ Manifests $\to$ Data Files) | Enterprise data lakehouse, batch/streaming ETL, cross-engine interoperability (Spark, Trino, StarRocks, Snowflake) |
| **Apache Paimon** | Streaming-First Lakehouse Format | **LSM-Tree on Cloud Object Storage** (Multi-level SSTs + MemTable + Changelog) | High-throughput streaming CDC sync, real-time upserts, lakehouse streaming storage bus (replacing Kafka tier) |
| **Apache Hive** | The Legacy SQL-on-Hadoop Format | **Directory-as-Partition Mapping** (Hive Metastore directory pointers) | Legacy Hadoop data warehousing, backward compatibility, baseline catalog metadata |

---

## 2. Metadata Architecture & Tracking Granularity

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                             APACHE HIVE METADATA MODEL                                  │
│  [ Hive Metastore (RDBMS) ] ──► Points to Directory: s3://lake/orders/dt=2025-01-15/    │
│  • Engine must execute slow recursive S3 LIST operations to discover files in directory.│
│  • Cost: O(N) where N is number of files/partitions.                                    │
└─────────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                            APACHE ICEBERG METADATA MODEL                                │
│  [ Catalog Pointer ] ──► [ vN.metadata.json ] ──► [ Manifest List ] ──► [ Manifests ]  │
│  • Tracks individual data files explicitly with column min/max stats and byte ranges.   │
│  • Cost: O(1) local manifest scan; zero S3 LIST calls during query planning.            │
└─────────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                            APACHE PAIMON METADATA MODEL                                 │
│  [ Catalog ] ──► [ Snapshot ] ──► [ Manifest List ] ──► [ LSM Levels & SST Buckets ]    │
│  • Tracks sorted string tables (SSTs) organized into hierarchical LSM levels (L0..LN).   │
│  • Cost: O(log N) primary key lookups and fast point/range scans.                       │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. ACID Guarantees & Concurrency Control

- **Apache Iceberg:** Uses **Optimistic Concurrency Control (OCC)** at snapshot commit time. Supports concurrent multi-engine writes (e.g., Flink streaming ingestion alongside a Spark batch compaction) with automated conflict detection and re-basing.
- **Apache Paimon:** Uses **Two-Phase Commit (2PC)** coordinated directly with Flink/Spark checkpoint barriers. Snapshots are committed atomically every checkpoint cycle without lock contention.
- **Apache Hive:** Relies on Hive ACID (ORC-only delta files with table/partition-level locks stored in the Metastore DB). Fragile in multi-engine environments.

---

## 4. Streaming Ingestion, Upsert & CDC Performance

```
                                  [ REAL-TIME STREAMING CDC INGESTION ]
                                                    │
                 ┌──────────────────────────────────┴──────────────────────────────────┐
                 ▼                                                                     ▼
    [ APACHE ICEBERG (MOR/COW) ]                                          [ APACHE PAIMON (LSM) ]
    • Writes Positional/Equality deletes                                  • Writes sorted SSTs to Level 0 (MemTable flush)
    • Commits every 30s-60s                                               • Commits every 10s-30s
    • High scan merge overhead under millions of updates                  • Background compaction merges levels asynchronously
    • Moderate streaming upsert throughput                                • **Extreme streaming upsert throughput (5x-10x)**
```

- **Paimon's Streaming Advantage:** Because Paimon uses an LSM-tree, streaming writes are pure sequential appends to $L_0$ memory buffers, avoiding immediate file rewriting or equality delete joins.
- **Native Changelog Feed:** Paimon can automatically produce `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, and `DELETE` streams directly from S3 storage, enabling downstream streaming jobs to consume real-time deltas without an intermediate Kafka topic.

---

## 5. Row-Level Deletes & Mutation Mechanics

| Feature | Apache Iceberg | Apache Paimon | Apache Hive |
| :--- | :--- | :--- | :--- |
| **Delete Implementation** | Positional Delete Files & Equality Delete Files | Deletion Vectors (Roaring Bitmaps) & LSM Level Merges | ORC Delete Delta Files |
| **Partial Column Updates** | Requires reading and rewriting full row | **Native in-storage merge** (`merge-engine=partial-update`) | Not supported |
| **Compaction Strategy** | Explicit `rewrite_data_files` procedure | In-line or Dedicated Background Compaction Job | Hive Compactor daemon |

---

## 6. Partitioning Models: Directory vs. Hidden vs. LSM Bucketing

- **Hive (Directory Partitioning):** Explicit string values mapped to filesystem directories (`year=2025/month=01/`).
  - *Pitfall:* Changing partition schemes requires a full table rewrite.
- **Iceberg (Hidden Partitioning):** Declarative transforms on existing columns (`day(created_at)`).
  - *Advantage:* Queries filter natural timestamps (`created_at >= '2025-01-01'`) without knowing the partition scheme. Supports **Partition Evolution** (zero historical data rewrite).
- **Paimon (LSM Bucketing):** Combines partition keys with deterministic hash bucketing (`bucket(16, id)`) or dynamic auto-scaling bucketing.

---

## 7. Schema Evolution & Data Safety

- **Hive:** Schema is defined solely in the Metastore. Changing column order or renaming columns can cause queries to misinterpret underlying binary Parquet/ORC columns, risking silent data corruption.
- **Iceberg:** Every column is tracked by an immutable **Unique Integer ID**. Column renames, additions, drops, and reorders are 100% metadata operations with zero data file rewrites and zero corruption risk.
- **Paimon:** Implements full schema evolution compatible with Flink SQL and Spark SQL.

---

## 8. Multi-Engine Interoperability Ecosystem

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                 ECOSYSTEM MATRIX                                        │
├───────────────────────┬─────────────────────────┬───────────────────┬───────────────────┤
│ Engine                │ Apache Iceberg          │ Apache Paimon     │ Apache Hive       │
├───────────────────────┼─────────────────────────┼───────────────────┼───────────────────┤
│ **Apache Flink**      │ Full Read/Write (2PC)   │ **Native Core**   │ Read / Write      │
│ **Apache Spark**      │ **Industry Standard**   │ Full Read/Write   │ Native / Core     │
│ **Trino**             │ **First-Class**         │ Read Support      │ Legacy Connector  │
│ **StarRocks**         │ **First-Class Native**  │ Native Connector  │ External Catalog  │
│ **Snowflake**         │ External Iceberg Tables │ Via Iceberg Sync  │ External Tables   │
│ **Google BigQuery**   │ BigLake Iceberg         │ Limited           │ External Tables   │
└───────────────────────┴─────────────────────────┴───────────────────┴───────────────────┘
```

---

## 9. Comprehensive Comparison Matrix

| Architectural Dimension | Apache Iceberg | Apache Paimon | Apache Hive |
| :--- | :--- | :--- | :--- |
| **Primary Architecture** | Snapshot Manifest Tree | LSM-Tree on Cloud Object Store | Directory-Mapped Metastore |
| **Storage Medium** | S3, ADLS, GCS, HDFS | S3, ADLS, GCS, HDFS | HDFS (Optimized), S3 (Limited) |
| **Default File Format** | Apache Parquet (or ORC) | Apache Parquet (or ORC) | Apache ORC (or Parquet/Text) |
| **Streaming CDC Upserts** | Moderate (Merge-on-Read) | **Superior (LSM-Tree Flush)** | Poor (Batch overwrite) |
| **Point Query Lookups** | Moderate (File scans) | **Very Fast (LSM Key Index)**| Slow (Full table scans) |
| **Batch OLAP Scan Speed** | **Fastest (Direct Columnar)**| Fast (LSM Merge Iterator) | Fast (ORC Vectorized) |
| **Hidden Partitioning** | **Yes** | No (Explicit partitions) | No (Explicit directories) |
| **Partition Evolution** | **Yes** (Zero data rewrite) | Limited | No |
| **Time Travel & Rollback** | **Yes** (`FOR SYSTEM_TIME AS OF`)| **Yes** (Snapshot Tags) | No |
| **Changelog Generation** | Limited | **Native Built-In** | No |
| **Data Governance / Branching**| **Yes** (Nessie / REST Git branches)| Tagging | No |

---

## 10. Architecture Decision Framework (When to Choose Which)

```
                                [ WHAT IS YOUR PRIMARY REQUIREMENT? ]
                                                  │
                 ┌────────────────────────────────┼────────────────────────────────┐
                 ▼                                ▼                                ▼
     [ ENTERPRISE LAKEHOUSE & ]       [ REAL-TIME STREAMING CDC & ]     [ LEGACY HADOOP / ]
     [ MULTI-ENGINE ANALYTICS ]       [ REAL-TIME UPSERT STREAM  ]     [ BASELINE METASTORE ]
                 │                                │                                │
                 ├─ Universal engine compatibility├─ Sub-minute streaming upserts  ├─ Existing on-prem Hadoop
                 │  (Spark, Trino, Snowflake)     ├─ Serving as streaming bus      │  HDFS infrastructure
                 ├─ Hidden partitioning           │  (replacing Kafka topic tiers) ├─ Hive Metastore as catalog
                 ├─ Heavy batch ETL & dbt models  ├─ High-velocity CDC sync from DB│  for external engines
                 ├─ Time travel & data branching  ├─ Partial-column updates        │
                 │                                │                                │
                 ▼                                ▼                                ▼
         CHOOSE: **ICEBERG**              CHOOSE: **PAIMON**               CHOOSE: **HIVE**
```

---

## 11. References & Further Reading

1. **Apache Iceberg Table Spec:** [https://iceberg.apache.org/spec/](https://iceberg.apache.org/spec/)
2. **Apache Paimon Architecture:** [https://paimon.apache.org/docs/master/concepts/basic-concepts/](https://paimon.apache.org/docs/master/concepts/basic-concepts/)
3. **Apache Hive Language Manual:** [https://cwiki.apache.org/confluence/display/Hive/LanguageManual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)
4. **Lakehouse Table Formats Benchmark Comparison:** [https://iceberg.apache.org/](https://iceberg.apache.org/)
