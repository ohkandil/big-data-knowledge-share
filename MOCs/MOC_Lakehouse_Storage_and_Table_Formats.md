---
title: "MOC: Lakehouse Storage & Table Formats"
type: moc
tags:
  - moc
  - lakehouse
  - table-formats
  - storage
  - iceberg
  - paimon
  - hive
aliases:
  - Storage MOC
  - Table Formats MOC
---

# 🧊 MOC: Lakehouse Storage & Table Formats

> Map of Content for open lakehouse table storage formats, transactional metadata trees, LSM storage engines, and catalog architectures on cloud object storage.

Parent: [[MOCs/00_Root_Streamhouse_MOC|Root Streamhouse MOC]]

---

## 🏗️ Core Technologies in this Domain

### 1. 🧊 [[Apache Iceberg/01_Overview|Apache Iceberg]] (The Universal Lakehouse Standard)
- **Primary Mission:** High-performance, open table format providing snapshot isolation, hidden partitioning, and full schema evolution on S3/ADLS.
- **Data Guide:** [[Apache Iceberg/02_Data_Guide|Storage Layout, File Types & Row-Level Deletes]]
- **Architecture:** [[Apache Iceberg/03_Architecture|Metadata Hierarchy, REST Catalog & OCC Commits]]
- **Performance:** [[Apache Iceberg/04_Performance_Guide|Compaction (rewrite_data_files), Z-Ordering & Snapshots]]

### 2. 🌊 [[Apache Paimon/01_Overview|Apache Paimon]] (Streaming-First Lakehouse Format)
- **Primary Mission:** LSM-tree on object storage designed for high-frequency streaming CDC upserts, partial-column updates, and changelog generation.
- **Data Guide:** [[Apache Paimon/02_Data_Guide|LSM Levels, Merge Engines & Changelog Producers]]
- **Architecture:** [[Apache Paimon/03_Architecture|Metadata Tree & Flink 2PC Streaming Commits]]
- **Performance:** [[Apache Paimon/04_Performance_Guide|LSM Sizing, Dedicated Compaction Jobs & Buffers]]

### 3. 🐝 [[Apache Hive/01_Overview|Apache Hive]] (The Foundation of SQL-on-Hadoop)
- **Primary Mission:** Declarative SQL over distributed storage; creator of the universal **Hive Metastore (HMS)** standard.
- **Data Guide:** [[Apache Hive/02_Data_Guide|Directory Partitioning, SerDe & ORC Formats]]
- **Architecture:** [[Apache Hive/03_Architecture|HS2, HMS Schema & Apache Tez Engine]]
- **Performance:** [[Apache Hive/04_Performance_Guide|Direct SQL Tuning & Migration to Apache Iceberg]]

---

## ⚖️ Cross-Format Comparison
- Read the definitive architectural breakdown: **[[Comparisons/Table_Formats_Iceberg_vs_Paimon_vs_Hive|Iceberg vs. Paimon vs. Hive]]**

---

## 🔗 Key Conceptual Connections
- Optimistic Concurrency Control across multiple engines: [[Concepts/Optimistic_Concurrency_Control_OCC|Optimistic Concurrency Control (OCC)]]
- How LSM-trees enable sub-minute streaming upserts: [[Concepts/LSM_Trees_and_Compaction|LSM-Trees & Compaction]]
- Columnar storage formats and file pruning: [[Concepts/Columnar_Storage_and_MinMax_Stats|Columnar Storage & Min/Max Stats]]
- Decoupling compute engines from storage layers: [[Concepts/Decoupled_Compute_and_Storage|Decoupled Compute & Storage]]

---

## 🔍 Upstream Writers & Downstream Readers
- **Writers:** [[Apache Flink/01_Overview|Apache Flink]] · [[Apache Kafka/01_Overview|Apache Kafka]]
- **Readers & Engines:** [[Trino/01_Overview|Trino]] · [[StarRocks/01_Overview|StarRocks]] · [[dbt/01_Overview|dbt]]
