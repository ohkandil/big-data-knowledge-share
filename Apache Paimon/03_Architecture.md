---
title: "Apache Paimon: Architecture"
type: architecture
tags:
  - apache-paimon
  - table-format
  - lsm-tree
  - streaming-lakehouse
  - cdc
  - changelog
  - 03-architecture
aliases:
  - "Paimon Architecture"
  - "Paimon Compaction"
  - "Paimon Bucket"
layer: "Streaming Lakehouse Table Format"
parent: "[[MOCs/MOC_Lakehouse_Storage_and_Table_Formats]]"
---

# Apache Paimon: Architecture & Commit Lifecycle

## Table of Contents
1. [Metadata Hierarchy on Object Storage](#1-metadata-hierarchy-on-object-storage)
2. [Streaming Commit Lifecycle (Two-Phase Commit with Flink)](#2-streaming-commit-lifecycle-two-phase-commit-with-flink)
3. [LSM Compaction Architecture](#3-lsm-compaction-architecture)
4. [Catalog Architecture: Filesystem, Hive, REST](#4-catalog-architecture-filesystem-hive-rest)
5. [Reader Architecture: Trino, StarRocks, Spark](#5-reader-architecture-trino-starrocks-spark)
6. [References & Further Reading](#6-references--further-reading)

---

## 1. Metadata Hierarchy on Object Storage

Paimon stores table files in a structured tree on cloud object storage:

```
s3://my-lakehouse-bucket/paimon/orders/
├── schema/
│   └── schema-0
├── snapshot/
│   ├── LATEST
│   ├── EARLIEST
│   ├── snapshot-1
│   └── snapshot-2
├── manifest/
│   ├── manifest-list-snapshot-2
│   └── manifest-file-xxx
└── dt=2025-01-15/
    ├── bucket-0/
    │   ├── data-xxx-0.parquet (Level 0)
    │   └── data-xxx-1.parquet (Level 1)
    └── bucket-1/
        └── data-yyy-0.parquet
```

- **`schema/`:** Schema definition files with full schema evolution tracking.
- **`snapshot/`:** Point-in-time snapshot pointers recording manifest list references and commit metadata.
- **`manifest/`:** Manifest lists and manifest files tracking active SST data files across all partitions and buckets.

---

## 2. Streaming Commit Lifecycle (Two-Phase Commit with Flink)

```
[ Flink Stream Source ] ──► [ Paimon Writer Task (Slot) ] ──► [ Paimon Committer (Single Task) ]
                                    │                                      │
                                    ▼ 1. Pre-Commit                        ▼ 2. Commit
                       Flushes SST files to S3 / L0          Atomically writes new Snapshot JSON
                       Emits commit message downstream       to S3 snapshot directory
```

1. **Phase 1 (Pre-Commit):**
   - Flink checkpoint barrier arrives at Paimon Writer tasks.
   - Writers flush in-memory `MemTable` to S3 as Level 0 Parquet SST files.
   - Writers send metadata descriptors (list of added/deleted files) downstream to the single Committer task.
2. **Phase 2 (Commit):**
   - Checkpoint completes successfully across all operators.
   - The Committer writes the new `manifest-list` and atomically creates `snapshot-N`, advancing the visible table state.

---

## 3. LSM Compaction Architecture

Compaction merges small SST files across LSM levels:
- **In-Line Compaction:** Performed directly by the Flink streaming writer task during execution.
- **Dedicated Compaction Cluster:** A separate standalone Flink or Spark job executes compactions asynchronously, completely decoupling CPU/disk I/O from the streaming write path.

---

## 4. Catalog Architecture: Filesystem, Hive, REST

- **Filesystem Catalog:** Uses directory path directly on S3/HDFS without external database dependencies.
- **Hive Catalog:** Integrates with the Hive Metastore (HMS), allowing Trino and Spark to discover Paimon tables automatically.
- **REST Catalog:** Cloud-native, standardized HTTP catalog service.

---

## 5. Reader Architecture: Trino, StarRocks, Spark

- **Vectorized Columnar Pushdown:** Connectors push filters and projected columns down into Parquet/ORC readers.
- **LSM Merge on Read:** For primary key tables, readers scan matching key files across levels, applying merge functions in memory using vectorized batch iterators.

---

## 6. References & Further Reading

1. **Paimon File System Layout:** [https://paimon.apache.org/docs/master/concepts/file-layout/](https://paimon.apache.org/docs/master/concepts/file-layout/)
2. **Paimon Flink Integration:** [https://paimon.apache.org/docs/master/engines/flink/](https://paimon.apache.org/docs/master/engines/flink/)
3. **Paimon Standalone Compaction:** [https://paimon.apache.org/docs/master/primary-key-table/compaction/](https://paimon.apache.org/docs/master/primary-key-table/compaction/)


---

**Layer:** 🌊 Streaming Lakehouse Table Format  
**Parent MOC:** [[MOCs/MOC_Lakehouse_Storage_and_Table_Formats|MOC: Lakehouse Storage & Table Formats]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** ← [[Apache Paimon/02_Data_Guide|Data Guide]]  |  → [[Apache Paimon/04_Performance_Guide|Performance Guide]]

**Related technologies:** [[Apache Iceberg/01_Overview|Apache Iceberg]] · [[Apache Hive/01_Overview|Apache Hive]] · [[Apache Flink/01_Overview|Apache Flink]] · [[Apache Kafka/01_Overview|Apache Kafka]]

**Core concepts:** [[Concepts/LSM_Trees_and_Compaction|LSM-Trees & Compaction]]
