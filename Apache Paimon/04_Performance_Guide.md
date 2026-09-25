---
title: "Apache Paimon: Performance Guide"
type: guide
tags:
  - apache-paimon
  - table-format
  - lsm-tree
  - streaming-lakehouse
  - cdc
  - changelog
  - 04-performance-guide
aliases:
  - "Paimon Performance"
  - "Paimon Tuning"
  - "Paimon Write Buffer"
layer: "Streaming Lakehouse Table Format"
parent: "[[MOCs/MOC_Lakehouse_Storage_and_Table_Formats]]"
---

# Apache Paimon: Performance Tuning & Production Optimization Guide

## Table of Contents
1. [LSM Memory & Buffer Sizing](#1-lsm-memory--buffer-sizing)
2. [Target File Size & Compaction Tuning](#2-target-file-size--compaction-tuning)
3. [Bucket Allocation & Scaling Strategies](#3-bucket-allocation--scaling-strategies)
4. [Asynchronous Dedicated Compaction Jobs](#4-asynchronous-dedicated-compaction-jobs)
5. [Changelog Performance Tuning](#5-changelog-performance-tuning)
6. [Common Anti-Patterns & Pitfalls](#6-common-anti-patterns--pitfalls)
7. [References & Further Reading](#7-references--further-reading)

---

## 1. LSM Memory & Buffer Sizing

```sql
-- Flink Table Properties for High-Throughput Streaming Ingest
CREATE TABLE paimon_orders (
    ...
) WITH (
    'write-buffer-size' = '256 mb',      -- Memory buffer per bucket before flushing to L0
    'write-buffer-spillable' = 'true',   -- Prevent TaskManager OOM by spilling write buffers
    'target-file-size' = '128 mb',       -- Target Parquet SST file size after compaction
    'num-sorted-run.compaction-trigger' = '5', -- Trigger minor compaction when L0 hits 5 files
    'num-levels' = '4'                   -- Total LSM tree depth
);
```

---

## 2. Target File Size & Compaction Tuning

- **Target File Size:** Maintain `target-file-size = 128 mb` to `256 mb` to ensure optimal scan performance for Trino and StarRocks.
- **Max Level Size:** Configure `num-levels = 4` to balance write amplification vs. read amplification.

---

## 3. Bucket Allocation & Scaling Strategies

- **Primary Key Tables (Fixed Bucketing):** Size buckets so that each bucket holds **1GB to 2GB** of active data.
- **Dynamic Bucketing (`'bucket' = '-1'`):** Use dynamic bucketing for workloads where partition data volume varies drastically across days.

---

## 4. Asynchronous Dedicated Compaction Jobs

In high-throughput enterprise pipelines, disable in-line compaction on writer tasks and run a dedicated compaction Flink job:

```sql
-- On writer job table definition:
'write-only' = 'true' -- Writer tasks only flush L0 files and commit snapshots
```

Run dedicated compaction job:
```bash
# Run standalone background compaction job
./bin/flink run \
    -c org.apache.paimon.flink.action.FlinkActions \
    paimon-flink-action.jar \
    compact \
    --warehouse s3://my-lakehouse/paimon \
    --database sales \
    --table orders
```

---

## 5. Changelog Performance Tuning

- If downstream jobs only require batch updates, set `'changelog-producer' = 'none'` to maximize write throughput.
- For high-frequency CDC streaming, use `'changelog-producer' = 'lookup'` and ensure TaskManagers have fast local NVMe SSDs configured for RocksDB lookups.

---

## 6. Common Anti-Patterns & Pitfalls

| Anti-Pattern | Why It Fails | Solution |
| :--- | :--- | :--- |
| **Too many buckets (>100 per partition for small data)** | Creates thousands of tiny SST files per checkpoint commit. | Reduce bucket count or switch to dynamic bucketing (`bucket = -1`). |
| **In-line compaction under heavy stream write load** | Writer tasks stall during major compaction, creating severe backpressure. | Set `'write-only' = 'true'` and deploy a dedicated compaction job. |
| **Small write buffers (<16MB)** | Triggers frequent tiny file flushes to S3, saturating S3 API rate limits. | Set `'write-buffer-size' = '128 mb'` or `'256 mb'`. |
| **Forgetting Snapshot Expiration** | Retains millions of historical snapshot files, slowing down catalog metadata reads. | Configure `'snapshot.time-retained' = '1 h'` and `'snapshot.num-retained.min' = '5'`. |

---

## 7. References & Further Reading

1. **Paimon Performance Tuning Guide:** [https://paimon.apache.org/docs/master/maintenance/configurations/](https://paimon.apache.org/docs/master/maintenance/configurations/)
2. **Paimon Dedicated Compaction Action:** [https://paimon.apache.org/docs/master/maintenance/dedicated-compaction/](https://paimon.apache.org/docs/master/maintenance/dedicated-compaction/)
3. **Paimon Flink Ingestion Optimization:** [https://paimon.apache.org/docs/master/primary-key-table/cdc-ingestion/](https://paimon.apache.org/docs/master/primary-key-table/cdc-ingestion/)


---

**Layer:** 🌊 Streaming Lakehouse Table Format  
**Parent MOC:** [[MOCs/MOC_Lakehouse_Storage_and_Table_Formats|MOC: Lakehouse Storage & Table Formats]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** ← [[Apache Paimon/03_Architecture|Architecture]]

**Related technologies:** [[Apache Iceberg/01_Overview|Apache Iceberg]] · [[Apache Hive/01_Overview|Apache Hive]] · [[Apache Flink/01_Overview|Apache Flink]] · [[Apache Kafka/01_Overview|Apache Kafka]]

**Core concepts:** [[Concepts/LSM_Trees_and_Compaction|LSM-Trees & Compaction]]
