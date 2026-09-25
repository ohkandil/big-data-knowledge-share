---
title: "Apache Iceberg: Overview & Foundational Concepts"
type: overview
tags:
  - apache-iceberg
  - table-format
  - lakehouse
  - snapshot
  - schema-evolution
  - hidden-partitioning
  - 01-overview
aliases:
  - "Iceberg"
  - "Apache Iceberg Overview"
layer: "Lakehouse Table Format"
parent: "[[MOCs/MOC_Lakehouse_Storage_and_Table_Formats]]"
---

# Apache Iceberg: Architectural Overview & Foundational Concepts

```
   ██╗ ██████╗███████╗██████╗ ███████╗██████╗  ██████╗ 
   ██║██╔════╝██╔════╝██╔══██╗██╔════╝██╔══██╗██╔════╝ 
   ██║██║     █████╗  ██████╔╝█████╗  ██████╔╝██║  ███╗
   ██║██║     ██╔══╝  ██╔══██╗██╔══╝  ██╔══██╗██║   ██║
   ██║╚██████╗███████╗██████╔╝███████╗██║  ██║╚██████╔╝
   ╚═╝ ╚═════╝╚══════╝╚═════╝ ╚══════╝╚═╝  ╚═╝ ╚═════╝ 
   Open Table Format for Huge Analytic Datasets
```

---

## Table of Contents
1. [Executive Summary & Core Mission](#1-executive-summary--core-mission)
2. [Historical Context & Genesis: Why Iceberg Was Created](#2-historical-context--genesis-why-iceberg-was-created)
   - [Netflix's Petabyte Data Lake & Hive Flaws](#netflixs-petabyte-data-lake--hive-flaws)
   - [File-Level vs. Directory-Level Tracking](#file-level-vs-directory-level-tracking)
   - [The Open Standard for Modern Lakehouses](#the-open-standard-for-modern-lakehouses)
3. [The Modern Streamhouse / Lakehouse Ecosystem Context](#3-the-modern-streamhouse--lakehouse-ecosystem-context)
   - [Positioning Iceberg as the Universal Storage Standard](#positioning-iceberg-as-the-universal-storage-standard)
   - [Multi-Engine Interoperability (Flink, Spark, Trino, StarRocks, Snowflake)](#multi-engine-interoperability-flink-spark-trino-starrocks-snowflake)
4. [High-Level Architectural Topology](#4-high-level-architectural-topology)
   - [The Snapshot Tree & Metadata Hierarchy](#the-snapshot-tree--metadata-hierarchy)
   - [Catalogs: REST, Nessie, Hive Metastore, AWS Glue, JDBC](#catalogs-rest-nessie-hive-metastore-aws-glue-jdbc)
5. [How Iceberg Processes Data at a High Level](#5-how-iceberg-processes-data-at-a-high-level)
   - [ACID Transactions via Optimistic Concurrency Control (OCC)](#acid-transactions-via-optimistic-concurrency-control-occ)
   - [Hidden Partitioning & Partition Evolution](#hidden-partitioning--partition-evolution)
   - [Full Schema Evolution (Safe Column ID Tracking)](#full-schema-evolution-safe-column-id-tracking)
   - [Time Travel & Version Rollbacks](#time-travel--version-rollbacks)
6. [Iceberg vs. Alternative Table Formats](#6-iceberg-vs-alternative-table-formats)
7. [Key Terminology & Mental Model Glossary](#7-key-terminology--mental-model-glossary)
8. [References & Further Reading](#8-references--further-reading)

---

## 1. Executive Summary & Core Mission

**Apache Iceberg** is an open-source, high-performance **table format for huge analytic datasets** stored on cloud object storage (Amazon S3, Azure ADLS, Google Cloud Storage) and distributed filesystems (HDFS).

Unlike legacy table structures (like Apache Hive) that manage tables as directories of files, Iceberg manages tables at the **individual file level** through an immutable, hierarchical metadata tree of snapshots, manifest lists, and manifest files. Iceberg brings full **ACID transactional semantics, deterministic multi-engine concurrency, hidden partitioning, schema evolution, and time travel** to modern data lakes—effectively turning raw S3 Parquet/ORC storage into a fully fledged, open enterprise data warehouse.

### Primary Capabilities:
- **Full ACID Transactions:** Serialized commits, snapshot isolation, and atomic multi-table updates using Optimistic Concurrency Control (OCC).
- **Hidden Partitioning:** Users query tables using natural timestamps without knowing the underlying physical partitioning scheme (e.g., `WHERE event_time >= '2025-01-01'` automatically prunes `year=2025/month=01/day=01` directories).
- **True Schema Evolution:** Add, drop, rename, update, or reorder columns safely without corrupting historical data or rewriting files (tracked via unique, immutable Column IDs).
- **Partition Evolution:** Change partition schemes over time (e.g., transition from monthly to daily partitioning) without rewriting existing historical partitions.
- **Time Travel & Rollback:** Query historical snapshots (`FOR SYSTEM_TIME AS OF`) or roll back accidental table corruptions in milliseconds.
- **Universal Multi-Engine Compatibility:** Seamless concurrent reading and writing across Apache Spark, Trino, StarRocks, Apache Flink, Snowflake, and BigQuery.

```
   [ QUERY & COMPUTE ENGINES ]
   • Apache Flink (Streaming Ingestion)  • Apache Spark (Batch ETL / ML)
   • Trino (Interactive Ad-Hoc SQL)      • StarRocks (Sub-Second OLAP)
   • Snowflake / BigQuery                • dbt (Data Modeling)
                     │                                   │
                     └─────────────────┬─────────────────┘
                                       │ Standard ANSI SQL / Iceberg API
                                       ▼
   ┌────────────────────────────────────────────────────────────────────────┐
   │                          APACHE ICEBERG TABLE                          │
   │  ┌──────────────────────────────────────────────────────────────────┐  │
   │  │ Catalog Layer (REST / Nessie / Glue / Hive Metastore / JDBC)     │  │
   │  └──────────────────────────────────┬───────────────────────────────┘  │
   │                                     │ Points to current snapshot       │
   │                                     ▼                                  │
   │  ┌──────────────────────────────────────────────────────────────────┐  │
   │  │ Metadata Tree (Metadata JSON ──► Manifest List ──► Manifests)    │  │
   │  └──────────────────────────────────┬───────────────────────────────┘  │
   │                                     │ Tracks individual data files     │
   │                                     ▼                                  │
   │  ┌──────────────────────────────────────────────────────────────────┐  │
   │  │ Data & Delete Layer (Parquet / ORC / Avro Files on S3 / ADLS)    │  │
   │  │ [ data-001.parquet ] [ data-002.parquet ] [ pos-delete-01.parquet]│  │
   │  └──────────────────────────────────────────────────────────────────┘  │
   └────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Historical Context & Genesis: Why Iceberg Was Created

### Netflix's Petabyte Data Lake & Hive Flaws

Around 2017, Netflix operated one of the world's largest cloud data lakes on Amazon S3 (over 10,000 tables, hundreds of petabytes). Their entire architecture relied on **Apache Hive table format conventions**:
1. **Directory-Based Table Tracking:** Hive defined a table as a directory and a partition as a subdirectory (e.g., `s3://netflix/views/date=2017-05-01/`).
2. **Expensive S3 List Calls (`O(N)`):** To plan a query over a year of data, query engines had to execute thousands of recursive S3 `LIST` API operations. Because S3 `LIST` operations are slow and rate-limited, query planning alone often took **5 to 20 minutes** before any data was actually read.
3. **No ACID Guarantees:** Updating a partition required deleting and replacing entire S3 directories. If an ETL job crashed halfway through, the table was left in a corrupt, half-written state. Concurrent writes caused silent data loss.
4. **Partition Lock-In:** Partitioning logic had to be explicitly embedded into user queries (e.g., `WHERE date_str = '2017-05-01'`). If the engineering team changed the partition column from string to epoch hour, every existing query and dashboard in the company broke.

### File-Level vs. Directory-Level Tracking

Ryan Blue and Dan Weeks at Netflix designed **Apache Iceberg** to replace directory-based tracking with **explicit file-level metadata tracking**:
- An Iceberg table is not a directory; it is a **canonical pointer to a metadata file**.
- Every single data file (Parquet/ORC) is tracked explicitly in an indexed manifest file along with its row count, lower/upper bounds, and null counts.
- **Result:** Query planning became an $O(1)$ local metadata read taking **10 milliseconds**, completely eliminating S3 `LIST` calls.

---

## 3. The Modern Streamhouse / Lakehouse Ecosystem Context

### Positioning Iceberg as the Universal Storage Standard

Iceberg has become the de-facto open standard for modern enterprise data lakehouses:

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         ENTERPRISE LAKEHOUSE ECOSYSTEM                                   │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│  STREAMING & BATCH WRITERS                                                               │
│  • Apache Flink: Continuous stream ingestion (2PC Checkpoints every 30s)                 │
│  • Apache Spark: Heavy batch transforms, ML pipelines, and dbt models                    │
│  • Debezium CDC: Change Data Capture streams syncing database WALs                       │
│                                           │                                              │
│                                           ▼ ACID Atomic Commits                          │
│  UNIVERSAL OPEN TABLE FORMAT LAYER                                                       │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                  APACHE ICEBERG                                    │  │
│  │  • Open standard metadata tree (Snapshots, Manifest Lists, Manifest Files)         │  │
│  │  • Multi-Engine Optimistic Concurrency Control (OCC)                               │  │
│  │  • Storage-level min/max column statistics & partition pruning                     │  │
│  │  • Row-level Positional and Equality Delete files                                  │  │
│  └────────────────────────────────────┬───────────────────────────────────────────────┘  │
│                                       │                                                  │
│                                       ▼ Zero-Copy Concurrent Multi-Engine Reads          │
│  ANALYTICAL QUERY ENGINES                                                                │
│  • Trino: Ad-hoc interactive data exploration & cross-source federation                  │
│  • StarRocks: Sub-second high-concurrency real-time OLAP serving                          │
│  • Snowflake / BigQuery: External Iceberg Tables reading directly from customer S3       │
│  • DuckDB: Embedded local analytics directly querying S3 Parquet files                   │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. High-Level Architectural Topology

```
                                [ CATALOG ]
                     (Points to Latest Metadata JSON)
                                     │
                                     ▼
                   ┌──────────────────────────────────┐
                   │       v3.metadata.json           │
                   │ • Table Schema (Column IDs: 1..N)│
                   │ • Current Snapshot: ID 1002      │
                   │ • Partition Spec: day(ts)        │
                   └─────────────────┬────────────────┘
                                     │ References
                                     ▼
                   ┌──────────────────────────────────┐
                   │    snap-1002.manifest-list       │
                   │ • Manifest File A (Partitions:01)│
                   │ • Manifest File B (Partitions:02)│
                   └────────┬─────────────────┬───────┘
                            │                 │
             ┌──────────────┘                 └──────────────┐
             ▼                                               ▼
┌───────────────────────────┐                   ┌───────────────────────────┐
│     manifest-A.avro       │                   │     manifest-B.avro       │
│ • data-1.parquet (Stats)  │                   │ • data-3.parquet (Stats)  │
│ • data-2.parquet (Stats)  │                   │ • delete-1.parquet (Delet)│
└────────────┬──────────────┘                   └────────────┬──────────────┘
             │                                               │
             ▼                                               ▼
 ┌───────────────────────┐                       ┌───────────────────────┐
 │ S3: data-1.parquet    │                       │ S3: data-3.parquet    │
 │ S3: data-2.parquet    │                       │ S3: delete-1.parquet  │
 └───────────────────────┘                       └───────────────────────┘
```

### The 3 Metadata Layers:
1. **Catalog Layer:** Stores the current pointer to `vN.metadata.json`. Catalogs include **REST Catalog** (standard), **Project Nessie** (Git-like branches for data), **AWS Glue**, **Hive Metastore**, or **JDBC**.
2. **Metadata File (`vN.metadata.json`):** Tracks table schema history, partition spec evolution, properties, and list of snapshots.
3. **Manifest List (`snap-N.avro`):** An array of manifest files comprising a specific snapshot, complete with partition range summaries for fast manifest pruning.
4. **Manifest File (`manifest-N.avro`):** Explicit list of data files and delete files, containing column-level upper/lower bounds, null counts, and byte sizes.

---

## 5. How Iceberg Processes Data at a High Level

### ACID Transactions via Optimistic Concurrency Control (OCC)

Iceberg uses **Optimistic Concurrency Control (OCC)** to support atomic, concurrent writes across multiple engines:
1. **Read Current State:** Writer reads the current table snapshot ID ($S_1$).
2. **Write Data Files:** Writer uploads new Parquet data files to S3.
3. **Create New Snapshot:** Writer generates a new manifest list referencing the new files and existing un-modified files ($S_2$).
4. **Atomic Commit:** Writer attempts to swap the catalog pointer from $S_1$ to $S_2$ using an atomic compare-and-swap operation.
5. **Conflict Resolution:** If another engine committed a snapshot ($S_{1.5}$) in the meantime, Iceberg checks if the two commits touch overlapping data files. If non-conflicting, Iceberg automatically re-bases and commits $S_2$ without failing.

### Hidden Partitioning & Partition Evolution

In Iceberg, users never create artificial partition columns (e.g., `event_date` string derived from `event_timestamp`).
- **Partition Transforms:** Define transforms directly on source columns: `year(ts)`, `month(ts)`, `day(ts)`, `hour(ts)`, `bucket(16, id)`, `truncate(4, name)`.
- **Query Transparent Pruning:** A query `WHERE ts >= '2025-01-15 00:00:00'` automatically prunes partition files based on the `day(ts)` partition spec.
- **Partition Evolution:** If you change partitioning from `month(ts)` to `day(ts)`, older data remains partitioned by month, newer data is written partitioned by day, and queries seamlessly prune across both partition layouts in a single query.

### Full Schema Evolution (Safe Column ID Tracking)

Iceberg assigns an immutable, unique **Integer Column ID** (e.g., `id: 1`, `name: 2`, `email: 3`) to every column:
- Renaming a column updates the metadata alias without touching Parquet data files.
- Dropping and re-adding a column with the same name assigns a new unique Column ID (e.g., `email: 4`), completely preventing accidental data resurrection bugs.

---

## 6. Iceberg vs. Alternative Table Formats

| Dimension | Apache Iceberg | Delta Lake | Apache Paimon | Apache Hive |
| :--- | :--- | :--- | :--- | :--- |
| **Catalog Architecture** | Canonical Metadata Pointer (REST / Nessie / Glue) | File-based directory (`_delta_log/`) | Canonical Snapshot pointer (REST / HMS) | Hive Metastore (HMS Directory mapping) |
| **Multi-Engine Neutrality** | **Industry Standard** (Flink, Spark, Trino, StarRocks, Snowflake) | Spark / Databricks First (UniForm bridges to Iceberg) | Flink / Streaming First | Legacy Hadoop Ecosystem |
| **Hidden Partitioning** | **Yes** (Native partition transforms) | No (Generated columns) | No (Explicit partitions) | No (Explicit directories) |
| **Partition Evolution** | **Yes** (Zero data rewrite) | Limited | Limited | No (Requires full table rewrite) |
| **Delete Strategies** | Positional Deletes & Equality Deletes | Deletion Vectors / COW | LSM Deletion Vectors | ORC Delta files |
| **Branching & Tagging** | **Yes** (Git-like branches via Nessie / REST) | Limited | Tagging supported | No |

---

## 7. Key Terminology & Mental Model Glossary

- **Snapshot:** An immutable point-in-time view of table state, composed of a manifest list and its associated data files.
- **Manifest List:** An Avro file containing metadata about all manifest files that make up a snapshot.
- **Manifest File:** An Avro file tracking an explicit list of data files (Parquet/ORC) and their column-level statistics.
- **REST Catalog:** The standardized open HTTP protocol for Iceberg catalog operations (supported by Tabular, Snowflake, AWS Glue, Unity Catalog).
- **Positional Delete:** A file specifying explicit row offset numbers to delete within a specific target Parquet file.
- **Equality Delete:** A file specifying key values (e.g., `id = 1005`) that should be deleted across all matching data files during query reads.
- **Copy-on-Write (COW):** Write mode where updates rewrite entire Parquet files with new data.
- **Merge-on-Read (MOR):** Write mode where updates write small delete files and append new rows, merging them during query execution.
- **Time Travel:** Querying historical data using snapshot IDs or timestamps (`FOR SYSTEM_TIME AS OF ...`).

---

## 8. References & Further Reading

1. **Official Apache Iceberg Documentation:** [https://iceberg.apache.org/docs/latest/](https://iceberg.apache.org/docs/latest/)
2. **The Iceberg Table Spec:** [https://iceberg.apache.org/spec/](https://iceberg.apache.org/spec/)
3. **Iceberg: The Definitive Guide (Book):** Blue, R., Weeks, D., & Thomas, J. (O'Reilly Media, 2024).
4. **Project Nessie (Git-like Catalog):** [https://projectnessie.org/](https://projectnessie.org/)


---

**Layer:** 🧊 Lakehouse Table Format  
**Parent MOC:** [[MOCs/MOC_Lakehouse_Storage_and_Table_Formats|MOC: Lakehouse Storage & Table Formats]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** → [[Apache Iceberg/02_Data_Guide|Data Guide]]

**Related technologies:** [[Apache Paimon/01_Overview|Apache Paimon]] · [[Apache Hive/01_Overview|Apache Hive]] · [[Apache Flink/01_Overview|Apache Flink]] · [[Trino/01_Overview|Trino]] · [[StarRocks/01_Overview|StarRocks]]

**Core concepts:** [[Concepts/LSM_Trees_and_Compaction|LSM-Trees & Compaction]]
