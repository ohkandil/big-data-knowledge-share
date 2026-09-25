---
title: "Apache Hive: Performance Guide"
type: guide
tags:
  - apache-hive
  - metastore
  - hive-metastore
  - batch
  - sql-on-hadoop
  - hiveql
  - 04-performance-guide
aliases:
  - "Hive Performance"
  - "Hive Tuning"
  - "Hive Vectorization"
layer: "Legacy Data Warehouse & Metastore"
parent: "[[MOCs/MOC_Lakehouse_Storage_and_Table_Formats]]"
---

# Apache Hive: Performance Tuning & Metastore Optimization Guide

## Table of Contents
1. [Hive Metastore (HMS) Scaling & Tuning](#1-hive-metastore-hms-scaling--tuning)
2. [Apache Tez Engine Tuning](#2-apache-tez-engine-tuning)
3. [ORC File Format Optimization](#3-orc-file-format-optimization)
4. [Vectorized Query Execution](#4-vectorized-query-execution)
5. [Mitigating the Small File Problem](#5-mitigating-the-small-file-problem)
6. [Common Anti-Patterns & Migration to Modern Lakehouses](#6-common-anti-patterns--migration-to-modern-lakehouses)
7. [References & Further Reading](#7-references--further-reading)

---

## 1. Hive Metastore (HMS) Scaling & Tuning

For enterprise clusters with millions of partitions:

```xml
<!-- hive-site.xml -->
<configuration>
  <!-- Enable direct SQL queries bypassing DataNucleus ORM for 10x faster partition fetches -->
  <property>
    <name>hive.metastore.try.direct.sql</name>
    <value>true</value>
  </property>
  
  <!-- Limit maximum partition count fetched in a single client call -->
  <property>
    <name>hive.metastore.limit.partition.request</name>
    <value>10000</value>
  </property>
  
  <!-- Connection pooling for backing MySQL/PostgreSQL metadata DB -->
  <property>
    <name>datanucleus.connectionPool.maxPoolSize</name>
    <value>50</value>
  </property>
</configuration>
```

---

## 2. Apache Tez Engine Tuning

```xml
<!-- Set Tez container memory allocation -->
<property>
  <name>hive.tez.container.size</name>
  <value>8192</value> <!-- 8 GB -->
</property>

<!-- Enable dynamic partition pruning in Tez -->
<property>
  <name>hive.tez.dynamic.partition.pruning</name>
  <value>true</value>
</property>

<!-- Group small split files into single Tez mapper task -->
<property>
  <name>tez.grouping.min-size</name>
  <value>134217728</value> <!-- 128 MB -->
</property>
```

---

## 3. ORC File Format Optimization

```sql
ALTER TABLE sales_db.orders SET TBLPROPERTIES (
    'orc.compress' = 'ZSTD',
    'orc.stripe.size' = '268435456', -- 256 MB Stripe size
    'orc.row.index.stride' = '10000',
    'orc.create.index' = 'true'
);
```

---

## 4. Vectorized Query Execution

```sql
-- Enable full vectorization in Hive on Tez
SET hive.vectorized.execution.enabled = true;
SET hive.vectorized.execution.reduce.enabled = true;
```

---

## 5. Mitigating the Small File Problem

```sql
-- Automatically merge small files at the end of a query
SET hive.merge.tezfiles = true;
SET hive.merge.size.per.task = 268435456; -- 256 MB
SET hive.merge.smallfiles.avgsize = 134217728; -- 128 MB
```

---

## 6. Common Anti-Patterns & Migration to Modern Lakehouses

| Anti-Pattern | Why It Fails | Modern Solution |
| :--- | :--- | :--- |
| **Over-partitioning (>10,000 subdirectories)** | HMS database crashes; Trino/Spark spend minutes scanning partitions. | Migrate to **Apache Iceberg** hidden partitioning (`day(ts)`). |
| **Unmanaged small files from streaming** | HDFS NameNode memory exhaustion; slow query execution. | Enable automatic file merging or use Iceberg/Paimon compaction. |
| **Using Text/CSV formats for analytics** | 10x higher storage cost, no min/max pushdowns, slow query speed. | Convert tables to ORC or Parquet. |

### Migration Path: In-Place Hive to Iceberg Migration
```sql
-- Spark SQL: Migrate Hive table in-place to Apache Iceberg without moving data files
CALL system.snapshot('hive_db.orders', 'iceberg_db.orders');
```

---

## 7. References & Further Reading

1. **Hive Performance Tuning Guide:** [https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties)
2. **Hive on Tez Tuning:** [https://cwiki.apache.org/confluence/display/TEZ/How+to+Tune+Tez](https://cwiki.apache.org/confluence/display/TEZ/How+to+Tune+Tez)
3. **Hive-to-Iceberg Migration:** [https://iceberg.apache.org/docs/latest/spark-procedures/#snapshot](https://iceberg.apache.org/docs/latest/spark-procedures/#snapshot)


---

**Layer:** 🐝 Legacy Data Warehouse & Metastore  
**Parent MOC:** [[MOCs/MOC_Lakehouse_Storage_and_Table_Formats|MOC: Lakehouse Storage & Table Formats]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** ← [[Apache Hive/03_Architecture|Architecture]]

**Related technologies:** [[Apache Iceberg/01_Overview|Apache Iceberg]] · [[Apache Paimon/01_Overview|Apache Paimon]] · [[Trino/01_Overview|Trino]] · [[StarRocks/01_Overview|StarRocks]]
