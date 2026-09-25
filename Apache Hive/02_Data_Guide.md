---
title: "Apache Hive: Data Guide"
type: guide
tags:
  - apache-hive
  - metastore
  - hive-metastore
  - batch
  - sql-on-hadoop
  - hiveql
  - 02-data-guide
aliases:
  - "Hive Data Guide"
  - "HiveQL"
  - "Hive SerDe"
layer: "Legacy Data Warehouse & Metastore"
parent: "[[MOCs/MOC_Lakehouse_Storage_and_Table_Formats]]"
---

# Apache Hive: Data Guide - Storage Layout, SerDe & Formats

## Table of Contents
1. [Physical Storage Layout on HDFS & Cloud Object Storage](#1-physical-storage-layout-on-hdfs--cloud-object-storage)
2. [Managed Tables vs. External Tables](#2-managed-tables-vs-external-tables)
3. [File Formats: ORC, Parquet, Avro, TextFile](#3-file-formats-orc-parquet-avro-textfile)
4. [The SerDe Architecture](#4-the-serde-architecture)
5. [Partitioning and Bucketing](#5-partitioning-and-bucketing)
6. [Hive ACID: Transactional Tables & Delta Compaction](#6-hive-acid-transactional-tables--delta-compaction)
7. [References & Further Reading](#7-references--further-reading)

---

## 1. Physical Storage Layout on HDFS & Cloud Object Storage

In Hive, table structures map directly to filesystem directory trees:

```
hdfs:///apps/hive/warehouse/sales.db/orders/
├── dt=2025-01-15/
│   ├── 000000_0.orc
│   └── 000001_0.orc
└── dt=2025-01-16/
    └── 000000_0.orc
```

---

## 2. Managed Tables vs. External Tables

| Feature | Managed Table (Internal) | External Table (`EXTERNAL`) |
| :--- | :--- | :--- |
| **Data Ownership** | Hive manages metadata and physical files. | Hive manages metadata only; storage is external. |
| **`DROP TABLE` Behavior** | Deletes both metadata in HMS and data files on HDFS/S3. | Deletes metadata in HMS; **leaves data files intact**. |
| **Default Location** | `/user/hive/warehouse/<db>/<table>` | User-defined S3 or HDFS path (`LOCATION 's3://...'`) |
| **Best Practice** | Avoid in enterprise lakehouses to prevent accidental data loss. | **Enterprise standard** for multi-engine access. |

```sql
-- Standard Enterprise External Table Definition
CREATE EXTERNAL TABLE sales_db.orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_amount DECIMAL(10, 2),
    created_at TIMESTAMP
)
PARTITIONED BY (order_date STRING)
STORED AS ORC
LOCATION 's3://my-enterprise-lake/sales/orders/';
```

---

## 3. File Formats: ORC, Parquet, Avro, TextFile

- **Apache ORC (Optimized Row Columnar):** Native to Hive. Organized into 256MB **Stripes**, containing index data, row data, and footer with lightweight min/max statistics.
- **Apache Parquet:** Industry-standard columnar format with cross-platform support.
- **Apache Avro:** Compact binary row-based format with full schema evolution support.
- **TextFile / CSV / JSON:** Uncompressed or gzip-compressed plain text.

---

## 4. The SerDe Architecture

Hive uses the **SerDe (Serializer/Deserializer)** interface to interpret custom file formats without modifying query execution code:

```sql
-- Custom JSON SerDe Table
CREATE EXTERNAL TABLE web_logs (
    ip_address STRING,
    request_url STRING,
    response_code INT
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://my-lake/logs/';
```

---

## 5. Partitioning and Bucketing

- **Partitioning:** Slices data into subdirectories by discrete partition keys (`dt=2025-01-15/`).
- **Bucketing:** Hashes records into fixed file buckets (`CLUSTERED BY (customer_id) INTO 16 BUCKETS`). Accelerates map-side bucket joins.

---

## 6. Hive ACID: Transactional Tables & Delta Compaction

Hive 3 introduced ACID transactional tables on HDFS:
- **Base Files:** Initial bulk data written to `base_000001/`.
- **Delta Files:** Inserts, updates, and deletes are written to `delta_000002_000002/` and `delete_delta_000002_000002/`.
- **Compactor:** Background daemon runs minor and major compactions to merge delta files into clean base files.

---

## 7. References & Further Reading

1. **Hive Storage Layout:** [https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL)
2. **Hive ORC File Format:** [https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC)
3. **Hive ACID Transactions:** [https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions)


---

**Layer:** 🐝 Legacy Data Warehouse & Metastore  
**Parent MOC:** [[MOCs/MOC_Lakehouse_Storage_and_Table_Formats|MOC: Lakehouse Storage & Table Formats]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** ← [[Apache Hive/01_Overview|Overview & Foundational Concepts]]  |  → [[Apache Hive/03_Architecture|Architecture]]

**Related technologies:** [[Apache Iceberg/01_Overview|Apache Iceberg]] · [[Apache Paimon/01_Overview|Apache Paimon]] · [[Trino/01_Overview|Trino]] · [[StarRocks/01_Overview|StarRocks]]
