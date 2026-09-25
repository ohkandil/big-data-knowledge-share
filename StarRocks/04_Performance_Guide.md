---
title: "StarRocks: Performance Guide"
type: guide
tags:
  - starrocks
  - olap
  - vectorized
  - real-time-analytics
  - materialized-views
  - simd
  - 04-performance-guide
aliases:
  - "StarRocks Performance"
  - "StarRocks Tuning"
  - "StarRocks MV"
layer: "Real-Time OLAP Engine"
parent: "[[MOCs/MOC_Transformation_and_OLAP_Serving]]"
---

# StarRocks: Performance Tuning & Production Optimization Guide

## Table of Contents
1. [Hardware Planning & Resource Allocation](#1-hardware-planning--resource-allocation)
2. [Table Design Optimization: Bucketing & Partitioning](#2-table-design-optimization-bucketing--partitioning)
3. [Primary Key Memory & Storage Optimization](#3-primary-key-memory--storage-optimization)
4. [Data Cache Sizing for Lakehouse Query Acceleration](#4-data-cache-sizing-for-lakehouse-query-acceleration)
5. [Async Materialized View Tuning & Refresh Strategies](#5-async-materialized-view-tuning--refresh-strategies)
6. [Pipeline Engine & Thread Concurrency Tuning](#6-pipeline-engine--thread-concurrency-tuning)
7. [Common Anti-Patterns & Pitfalls](#7-common-anti-patterns--pitfalls)
8. [Monitoring & Diagnostics](#8-monitoring--diagnostics)
9. [References & Further Reading](#9-references--further-reading)

---

## 1. Hardware Planning & Resource Allocation

### Recommended Node Configurations:
- **Backend (BE) Nodes:**
  - **CPU:** 32–64 vCPUs with AVX2 or AVX-512 support.
  - **RAM:** 128GB–256GB RAM.
  - **Storage:** Local NVMe SSDs (dedicated storage for tablets and data cache).
- **Frontend (FE) Nodes:**
  - **CPU:** 16 vCPUs.
  - **RAM:** 64GB RAM (allocate ~32GB to JVM heap via `fe.conf`: `JAVA_OPTS="-Xmx32g -Xms32g"`).

---

## 2. Table Design Optimization: Bucketing & Partitioning

### The Golden Rules of Bucketing:
1. **Target Tablet Size:** Size your partitions and buckets so that each tablet is **between 100MB and 10GB** (ideal: 1GB to 5GB).
2. **Bucket Count Formula:**
   $$\text{Num Buckets} = \frac{\text{Compressed Data Size per Partition}}{1\text{ GB to }5\text{ GB}}$$
3. **Avoid Over-Partitioning:** Having 1,000,000 tiny tablets (<10MB each) causes severe metadata memory bloat on FE nodes and ruins vectorized scan efficiency.

```sql
-- Optimized StarRocks Table Definition
CREATE TABLE fct_web_events (
    event_timestamp DATETIME,
    user_id BIGINT,
    event_type VARCHAR(50),
    page_url VARCHAR(255),
    response_time_ms INT
) ENGINE=OLAP
DUPLICATE KEY(event_timestamp, user_id)
PARTITION BY date_trunc('day', event_timestamp)
DISTRIBUTED BY HASH(user_id) BUCKETS 32
PROPERTIES (
    "replication_num" = "3",
    "bloom_filter_columns" = "page_url",
    "compression" = "ZSTD"
);
```

---

## 3. Primary Key Memory & Storage Optimization

By default, Primary Key tables store primary key indexes in **RAM** on BE nodes for maximum upsert speed.

### Enabling Persistent Index (Disk-backed RocksDB Index):
For large tables (>100M rows), in-memory indexes can consume excessive RAM. Enable the **Persistent Index** to move primary key indexes to local NVMe SSDs:

```sql
ALTER TABLE users_cdc SET ("enable_persistent_index" = "true");
```
- **Memory Savings:** Reduces RAM consumption by **80%** with only a 5–10% drop in streaming upsert throughput.

---

## 4. Data Cache Sizing for Lakehouse Query Acceleration

Configure local NVMe cache in `be.conf`:
```properties
# be.conf
# Enable local data cache for S3/Iceberg tables
datacache.enable = true
datacache.disk_mount_points = /mnt/nvme1/datacache;/mnt/nvme2/datacache
datacache.mem_size = 16GB
datacache.disk_size = 1TB
```

---

## 5. Async Materialized View Tuning & Refresh Strategies

```sql
-- Incremental refresh partitioned by day
CREATE MATERIALIZED VIEW mv_hourly_aggregates
REFRESH ASYNC EVERY(INTERVAL 10 MINUTE)
PARTITION BY event_date
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES (
    "partition_refresh_number" = "3" -- Only refresh the latest 3 partitions on each run
)
AS SELECT ...
```

---

## 6. Pipeline Engine & Thread Concurrency Tuning

In `be.conf`:
```properties
# Number of CPU worker threads for pipeline engine (default: number of CPU cores)
pipeline_exec_thread_pool_size = 32

# Memory limit for a single query (default: 80% of BE memory)
query_mem_limit = 64GB
```

---

## 7. Common Anti-Patterns & Pitfalls

| Anti-Pattern | Why It Fails | Solution |
| :--- | :--- | :--- |
| **Thousands of Tiny Partitions / Buckets** | Massive tablet metadata overhead, high FE memory usage, slow query planning. | Consolidate partitions to daily/monthly and size tablets to 1GB–5GB. |
| **High-frequency Single-Row Inserts** | Creates millions of micro-segment files; causes compaction storms. | Batch inserts into micro-batches of $\ge 1,000$ rows or use Kafka Routine Load. |
| **Large Primary Key Tables without Persistent Index** | BE nodes run out of memory (OOM) holding millions of in-memory index keys. | Set `"enable_persistent_index" = "true"`. |
| **Ignoring Data Cache on Cloud S3** | Queries incur high object storage network latency and S3 GET API costs. | Enable `datacache.enable = true` on BE/CN nodes. |

---

## 8. Monitoring & Diagnostics

- **StarRocks Web UI:** Access FE at `http://<fe_host>:8030` for query profiler, tablet health, and node status.
- **Explain Analyze:** Run `EXPLAIN ANALYZE <sql_query>;` to view the full vectorized operator execution tree, SIMD efficiency, and scan I/O breakdown.
- **Prometheus Metrics:** Expose FE metrics (`http://<fe_host>:8030/metrics`) and BE metrics (`http://<be_host>:8040/metrics`).

---

## 9. References & Further Reading

1. **StarRocks Performance Tuning:** [https://docs.starrocks.io/docs/administration/management/resource_management/](https://docs.starrocks.io/docs/administration/management/resource_management/)
2. **Table Design Best Practices:** [https://docs.starrocks.io/docs/table_design/table_design_overview/](https://docs.starrocks.io/docs/table_design/table_design_overview/)
3. **Query Profile Analysis:** [https://docs.starrocks.io/docs/administration/query_profile_overview/](https://docs.starrocks.io/docs/administration/query_profile_overview/)


---

**Layer:** ⭐ Real-Time OLAP Engine  
**Parent MOC:** [[MOCs/MOC_Transformation_and_OLAP_Serving|MOC: Transformation & OLAP Serving]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** ← [[StarRocks/03_Architecture|Architecture]]

**Related technologies:** [[Trino/01_Overview|Trino]] · [[dbt/01_Overview|dbt]] · [[Apache Iceberg/01_Overview|Apache Iceberg]] · [[Apache Paimon/01_Overview|Apache Paimon]] · [[Apache Kafka/01_Overview|Apache Kafka]]

**Core concepts:** [[Concepts/LSM_Trees_and_Compaction|LSM-Trees & Compaction]]
