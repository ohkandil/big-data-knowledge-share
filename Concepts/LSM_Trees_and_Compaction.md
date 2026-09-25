---
title: "LSM-Trees & Compaction"
type: concept
tags:
  - concept
  - storage
  - paimon
  - starrocks
  - rocksdb
aliases:
  - LSM-Tree
  - Log-Structured Merge-Tree
  - Compaction
---

# 🌲 Concept: LSM-Trees & Compaction

> **Definition:** A Log-Structured Merge-Tree (LSM-Tree) is a disk-based storage structure that combines the benefits of log-structured storage and B-trees. LSM-Trees are used in various databases and file systems, including [[Apache Paimon/01_Overview|Paimon]], [[StarRocks/01_Overview|StarRocks]], and Flink's RocksDB state backend.

## The Core Problem It Solves

High write throughput and low latency: LSM-Trees are designed to handle high volumes of writes while maintaining low latency. They achieve this by separating the write path from the read path and using a log-structured approach to store data.

## How It Works

```
LSM-Tree Structure:

    L0 (Memory Buffer)
    └── L1 (Disk-Based Level 1)
        └── L2 (Disk-Based Level 2)
            └── ...
            └── LN (Disk-Based Level N)

Write Path:

    Write to L0 (Memory Buffer)
    Flush L0 to L1 (Disk-Based Level 1)
    Merge L1 and L2 (Disk-Based Level 2)
    Merge L2 and L3 (Disk-Based Level 3)
    ...
    Merge LN and LN+1 (Disk-Based Level N+1)

Read Path:

    Read from L0 (Memory Buffer)
    Read from L1 (Disk-Based Level 1)
    Read from L2 (Disk-Based Level 2)
    ...
    Read from LN (Disk-Based Level N)
```

- **Write-ahead logging:** All writes are first written to the L0 memory buffer, which provides high performance and low latency.
- **Compaction:** The L0 memory buffer is periodically flushed to disk, and the levels are merged to reduce the number of disk seeks and improve read performance.
- **Merge:** The merge process involves reading data from multiple levels and writing it to a single level, which reduces the number of levels and improves read performance.

## Where It Appears in This Vault

- [[Apache Paimon/02_Data_Guide|Paimon Data Guide]] — Paimon uses LSM-Trees to store data on disk, providing high write throughput and low latency.
- [[StarRocks/02_Data_Guide|StarRocks Data Guide]] — StarRocks uses LSM-Trees to store data on disk, providing high write throughput and low latency.
- [[Concepts/Exactly_Once_Semantics_and_2PC|Exactly-Once Semantics]] — LSM-Trees are used to ensure exactly-once semantics in distributed systems.

## Mental Model

LSM-Trees are designed to handle high volumes of writes while maintaining low latency. They achieve this by separating the write path from the read path and using a log-structured approach to store data. The write-ahead logging and compaction processes ensure that data is written to disk in a way that minimizes disk seeks and improves read performance.
