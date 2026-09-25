---
title: "Apache Iceberg: Data Guide"
type: guide
tags:
  - apache-iceberg
  - table-format
  - lakehouse
  - snapshot
  - schema-evolution
  - hidden-partitioning
  - 02-data-guide
aliases:
  - "Iceberg Data Guide"
  - "Iceberg Snapshots"
  - "Iceberg Manifest"
layer: "Lakehouse Table Format"
parent: "[[MOCs/MOC_Lakehouse_Storage_and_Table_Formats]]"
---

# Apache Iceberg: Data Guide - Storage Structure & Row-Level Operations

## Table of Contents
1. [Physical Storage Layout & Metadata Tree](#1-physical-storage-layout--metadata-tree)
2. [Data Files & Supported Formats (Parquet, ORC, Avro)](#2-data-files--supported-formats-parquet-orc-avro)
3. [Row-Level Updates & Deletes: Copy-On-Write (COW) vs. Merge-On-Read (MOR)](#3-row-level-updates--deletes-copy-on-write-cow-vs-merge-on-read-mor)
4. [Positional Deletes vs. Equality Deletes](#4-positional-deletes-vs-equality-deletes)
5. [Hidden Partitioning Transforms](#5-hidden-partitioning-transforms)
6. [Schema Evolution Internals](#6-schema-evolution-internals)
7. [Time Travel & Snapshot Branching](#7-time-travel--snapshot-branching)
8. [References & Further Reading](#8-references--further-reading)

---

## 1. Physical Storage Layout & Metadata Tree

An Apache Iceberg table directory consists of two logical directories on object storage:
1. **`metadata/`:** Contains JSON table metadata, Avro manifest lists, and Avro manifest files.
2. **`data/`:** Contains raw columnar Parquet or ORC data files and delete files.

```
s3://analytics-lakehouse/sales_db/orders/
├── metadata/
│   ├── v1.metadata.json
│   ├── v2.metadata.json
│   ├── snap-1001-1-manifest-list.avro
│   ├── snap-1002-1-manifest-list.avro
│   ├── 00000-1-manifest.avro
│   └── 00001-1-manifest.avro
└── data/
    ├── order_date_day=2025-01-15/
    │   ├── 00000-10-data.parquet
    │   ├── 00001-10-data.parquet
    │   └── 00002-10-deletes-pos.parquet
    └── order_date_day=2025-01-16/
        └── 00000-11-data.parquet
```

---

## 2. Data Files & Supported Formats

- **Apache Parquet (Default & Recommended):** Industry standard columnar format with dictionary encoding, run-length encoding, and page-level min/max statistics.
- **Apache ORC:** Popular in Hive/Trino ecosystems; highly optimized compression.
- **Apache Avro:** Row-based format used for Iceberg internal metadata files and streaming write buffers.

---

## 3. Copy-On-Write (COW) vs. Merge-On-Read (MOR)

| Feature | Copy-On-Write (COW) | Merge-On-Read (MOR) |
| :--- | :--- | :--- |
| **Update / Delete Mechanism** | Rewrites the entire Parquet file containing modified rows. | Appends small Positional or Equality Delete files without touching original data files. |
| **Write Latency** | High (expensive multi-MB writes). | **Sub-second / Low** (ideal for streaming CDC). |
| **Read Latency** | **Fastest** (pure sequential columnar scan). | Slower (requires merging delete bitmaps during scan). |
| **Best For** | Heavy batch ETL pipelines and reporting tables. | High-frequency Flink/Spark streaming ingestion. |

---

## 4. Positional Deletes vs. Equality Deletes

### 1. Positional Delete Files
Specifies the exact file path and row position within that file:
```
File: s3://.../00000-10-data.parquet | Pos: 4501
File: s3://.../00000-10-data.parquet | Pos: 8920
```
- **Read Speed:** Fast (Engine reads bitmap index and skips rows during Parquet decode).

### 2. Equality Delete Files
Specifies field equality conditions (e.g., `user_id = 99201`):
- **Write Speed:** Very fast (no index lookup needed during write).
- **Read Speed:** Slower (query engine must evaluate join against delete keys during scan).

---

## 5. Hidden Partitioning Transforms

Iceberg supports declarative partition transforms without creating synthetic table columns:

| Transform | SQL Example | Resulting Directory Structure |
| :--- | :--- | :--- |
| **`year(ts)`** | `PARTITIONED BY (year(created_at))` | `created_at_year=2025/` |
| **`month(ts)`** | `PARTITIONED BY (month(created_at))` | `created_at_month=2025-01/` |
| **`day(ts)`** | `PARTITIONED BY (day(created_at))` | `created_at_day=2025-01-15/` |
| **`hour(ts)`** | `PARTITIONED BY (hour(created_at))` | `created_at_hour=2025-01-15-14/` |
| **`bucket(N, col)`** | `PARTITIONED BY (bucket(16, user_id))` | `user_id_bucket=7/` |
| **`truncate(L, col)`**| `PARTITIONED BY (truncate(2, zip_code))`| `zip_code_trunc=94/` |

---

## 6. Schema Evolution Internals

Iceberg maps every table column to a unique **Integer ID** assigned sequentially.

```sql
-- Initial Schema: (id:1 [name], id:2 [email])
ALTER TABLE orders RENAME COLUMN email TO contact_email;
-- Result: Schema metadata updates id:2 alias to contact_email. Parquet files unchanged.

ALTER TABLE orders DROP COLUMN contact_email;
-- Result: id:2 marked dropped.

ALTER TABLE orders ADD COLUMN contact_email VARCHAR;
-- Result: New column assigned id:3. Existing Parquet files read id:3 as NULL.
```

---

## 7. Time Travel & Snapshot Branching

```sql
-- Query snapshot from 2 hours ago
SELECT * FROM sales_db.orders 
FOR SYSTEM_TIME AS OF (CURRENT_TIMESTAMP - INTERVAL '2' HOUR);

-- Query specific Snapshot ID
SELECT * FROM sales_db.orders 
FOR SYSTEM_VERSION AS OF 8940192840192841;

-- Rollback accidental deletion
CALL system.rollback_to_snapshot('sales_db.orders', 8940192840192841);
```

---

## 8. References & Further Reading

1. **Iceberg File Layout:** [https://iceberg.apache.org/spec/#table-metadata](https://iceberg.apache.org/spec/#table-metadata)
2. **Iceberg Row-Level Updates:** [https://iceberg.apache.org/docs/latest/spark-writes/#row-level-deletes](https://iceberg.apache.org/docs/latest/spark-writes/#row-level-deletes)
3. **Iceberg Schema Evolution:** [https://iceberg.apache.org/docs/latest/evolution/](https://iceberg.apache.org/docs/latest/evolution/)


---

**Layer:** 🧊 Lakehouse Table Format  
**Parent MOC:** [[MOCs/MOC_Lakehouse_Storage_and_Table_Formats|MOC: Lakehouse Storage & Table Formats]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** ← [[Apache Iceberg/01_Overview|Overview & Foundational Concepts]]  |  → [[Apache Iceberg/03_Architecture|Architecture]]

**Related technologies:** [[Apache Paimon/01_Overview|Apache Paimon]] · [[Apache Hive/01_Overview|Apache Hive]] · [[Apache Flink/01_Overview|Apache Flink]] · [[Trino/01_Overview|Trino]] · [[StarRocks/01_Overview|StarRocks]]

**Core concepts:** [[Concepts/LSM_Trees_and_Compaction|LSM-Trees & Compaction]]
