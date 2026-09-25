---
title: "Trino: Data Guide"
type: guide
tags:
  - trino
  - federated-query
  - presto
  - sql
  - olap
  - connector-spi
  - 02-data-guide
aliases:
  - "Trino Data Guide"
  - "Trino Connectors"
  - "Trino Splits"
layer: "Federated Query Engine"
parent: "[[MOCs/MOC_Transformation_and_OLAP_Serving]]"
---

# Trino: Data Processing & Engine Mechanics

## Table of Contents
1. [The Connector Service Provider Interface (SPI)](#1-the-connector-service-provider-interface-spi)
2. [Data Hierarchy: Catalog, Schema, Table, Column](#2-data-hierarchy-catalog-schema-table-column)
3. [In-Memory Columnar Data Layout: Pages & Blocks](#3-in-memory-columnar-data-layout-pages--blocks)
4. [Split Generation & Partition Pruning](#4-split-generation--partition-pruning)
5. [The Lakehouse Connectors: Iceberg, Delta Lake, Hive](#5-the-lakehouse-connectors-iceberg-delta-lake-hive)
6. [Data Type System & Complex Type Processing](#6-data-type-system--complex-type-processing)
7. [References & Further Reading](#7-references--further-reading)

---

## 1. The Connector Service Provider Interface (SPI)

Trino achieves engine-storage separation via the **Connector SPI (Service Provider Interface)**. A connector translates Trino's internal execution primitives into native storage API calls.

### Core SPI Interfaces:
- **`ConnectorMetadata`:** Inspects schemas, tables, columns, statistics, and handles partition metadata.
- **`ConnectorSplitManager`:** Divides a table into parallel data chunks (`ConnectorSplit`).
- **`ConnectorPageSourceProvider`:** Reads splits from physical storage and streams columnar `Page` objects to the execution engine.
- **`ConnectorPageSinkProvider`:** Receives `Page` objects from the engine and writes them to target storage (e.g., writing Parquet files to S3).

```
   [ SQL Query ] ──► [ Trino Engine ]
                             │
            ┌────────────────┴────────────────┐
            ▼                                 ▼
   [ ConnectorMetadata ]           [ ConnectorSplitManager ]
   (Resolve table schemas & stats) (Partition pruning & splits)
            │                                 │
            └────────────────┬────────────────┘
                             ▼
              [ ConnectorPageSourceProvider ]
              (Streams Pages from S3 / JDBC / Kafka)
```

---

## 2. Data Hierarchy: Catalog, Schema, Table, Column

Trino enforces standard ANSI SQL 3-tier catalog namespace qualification:

```
                  [ TRINO CLUSTER ]
                          │
       ┌──────────────────┼──────────────────┐
       ▼                  ▼                  ▼
[ Catalog: iceberg ] [ Catalog: hive ] [ Catalog: postgres ]
       │
       ├─► [ Schema: core_sales ]
       │         │
       │         ├─► [ Table: orders ]
       │         │         │
       │         │         ├─► Column: order_id (BIGINT)
       │         │         ├─► Column: order_date (DATE)
       │         │         └─► Column: amount (DECIMAL)
       │         │
       │         └─► [ Table: customers ]
       │
       └─► [ Schema: marketing ]
```

Each catalog configuration file (e.g., `etc/catalog/iceberg.properties`) specifies its connector type and connection metadata:
```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://iceberg-catalog.internal:8443
hive.s3.endpoint=https://s3.us-east-1.amazonaws.com
```

---

## 3. In-Memory Columnar Data Layout: Pages & Blocks

Internally, Trino does **not** process data as row objects. Instead, it processes data in **columnar, vectorized batches** called **Pages**.

### Structure of a Page:
- A **Page** represents a rectangular 2D slice of data containing up to thousands of rows (capped by default at ~1MB or 16,000 rows).
- A Page consists of an array of **`Block`** objects—one `Block` per column.

```
┌────────────────────────────────────────────────────────────────────────┐
│                              TRINO PAGE                                │
├────────────────────────────────────────────────────────────────────────┤
│  Block 0 (order_id):    [ 1001, 1002, 1003, 1004, 1005 ... ] (LongArray)│
│  Block 1 (status):      [ 'OK', 'FAIL', 'OK', 'OK', 'FAIL' ] (SliceArray│
│  Block 2 (amount_usd):  [ 49.99, 12.50, 99.00, 5.00, 120.0 ] (DoubleArr │
│  Block 3 (is_active):   [  1,     0,     1,     1,     0   ] (ByteArray │
└────────────────────────────────────────────────────────────────────────┘
```

### Advantages of the Block / Page Architecture:
1. **CPU Cache Locality:** Scanning a single column accesses contiguous memory addresses, keeping CPU L1/L2 caches full.
2. **SIMD Vectorization:** Allows the JVM JIT compiler to compile loops into SIMD vector instructions (e.g., evaluating `amount > 50` on 8 values simultaneously).
3. **Zero Deserialization Overhead:** Blocks read directly from columnar formats (Parquet/ORC) require minimal translation.

---

## 4. Split Generation & Partition Pruning

When a query is planned, the **Split Manager** evaluates predicates to minimize I/O:

```
[ Table: orders (10,000 Parquet Files) ]
                   │
                   ▼ Predicate: WHERE order_date = DATE '2025-01-15'
[ Partition Pruner / Iceberg Manifest Evaluator ]
                   │
                   ▼ (Prunes 9,900 files using min/max stats)
[ 100 Matching Parquet Files ] ──► Split into 128MB byte-range Splits ──► Dispatched to Workers
```

1. **Partition Pruning:** Evaluates partition keys (e.g., `year=2025/month=01`) and eliminates entire subdirectories.
2. **File-Level Pruning (Min/Max Stats):** Reads Parquet/ORC footer statistics (or Iceberg manifest metrics) to skip files where `max(order_date) < '2025-01-15'`.
3. **Split Chunking:** Splits large files into discrete byte ranges processed independently by different worker threads.

---

## 5. The Lakehouse Connectors: Iceberg, Delta Lake, Hive

| Feature | Iceberg Connector | Delta Lake Connector | Hive Connector |
| :--- | :--- | :--- | :--- |
| **Catalog Mechanism** | REST, Nessie, Glue, Hive Metastore | Unity Catalog, Hive Metastore | Hive Metastore (HMS) / AWS Glue |
| **ACID Snapshots** | Full snapshot isolation | Transaction log (`_delta_log`) | Hive ACID (limited) |
| **Delete Support** | Equality & Positional Deletes | Deletion Vectors / COW | ORC Delta files |
| **Schema Evolution** | Full (safe column add, drop, rename, reorder) | Full schema evolution | Limited (risks data corruption on rename) |
| **Hidden Partitioning** | Supported (Transforms: `day()`, `bucket()`, `truncate()`) | Explicit column partitioning | Explicit directory partitions |

---

## 6. Data Type System & Complex Type Processing

Trino has a rich, native type system with high-performance operations on nested structures:

- **Row Types (Structs):** `ROW(street VARCHAR, city VARCHAR, zip INTEGER)`
- **Array Types:** `ARRAY<BIGINT>` (Supports functions: `transform()`, `filter()`, `reduce()`)
- **Map Types:** `MAP<VARCHAR, VARCHAR>`
- **JSON Type:** Native JSON parsing with `$path` operators.

```sql
-- Advanced lambda transformations on complex types
SELECT 
    order_id,
    transform(line_items, item -> item.price * item.quantity) AS item_totals,
    reduce(line_items, 0.0, (acc, item) -> acc + (item.price * item.quantity), acc -> acc) AS computed_total
FROM iceberg.sales.orders;
```

---

## 7. References & Further Reading

1. **Trino Connectors Guide:** [https://trino.io/docs/current/connector.html](https://trino.io/docs/current/connector.html)
2. **Trino Iceberg Connector:** [https://trino.io/docs/current/connector/iceberg.html](https://trino.io/docs/current/connector/iceberg.html)
3. **Trino Type System & Functions:** [https://trino.io/docs/current/functions.html](https://trino.io/docs/current/functions.html)


---

**Layer:** 🔍 Federated Query Engine  
**Parent MOC:** [[MOCs/MOC_Transformation_and_OLAP_Serving|MOC: Transformation & OLAP Serving]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** ← [[Trino/01_Overview|Overview & Foundational Concepts]]  |  → [[Trino/03_Architecture|Architecture]]

**Related technologies:** [[StarRocks/01_Overview|StarRocks]] · [[dbt/01_Overview|dbt]] · [[Apache Iceberg/01_Overview|Apache Iceberg]] · [[Apache Hive/01_Overview|Apache Hive]] · [[Apache Paimon/01_Overview|Apache Paimon]]
