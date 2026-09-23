# Trino: Performance Tuning & Production Optimization Guide

## Table of Contents
1. [Trino Memory Management Architecture](#1-trino-memory-management-architecture)
2. [Coordinator & Worker Resource Sizing](#2-coordinator--worker-resource-sizing)
3. [Join Strategy Optimization & Dynamic Filtering](#3-join-strategy-optimization--dynamic-filtering)
4. [Connector-Level Pushdowns & Lakehouse File Sizing](#4-connector-level-pushdowns--lakehouse-file-sizing)
5. [Query Concurrency & Resource Groups](#5-query-concurrency--resource-groups)
6. [Common Anti-Patterns & Pitfalls](#6-common-anti-patterns--pitfalls)
7. [Production Monitoring & Telemetry](#7-production-monitoring--telemetry)
8. [References & Further Reading](#8-references--further-reading)

---

## 1. Trino Memory Management Architecture

Trino manages worker memory to prevent Java heap OutOfMemoryErrors (OOM).

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      TRINO WORKER JVM HEAP (e.g. 64GB)                  │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ User Memory Pool (query.max-total-memory-per-node: e.g. 32GB)      │  │
│  │ (Hash tables for joins, group-by aggregates, window buffers)      │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ System Memory Pool                                                │  │
│  │ (Exchange buffers, network serialization, metadata)               │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ Untracked / Headroom Space                                        │  │
│  │ (Garbage collection overhead, JIT compiler, thread stacks)        │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

### Core Memory Configurations (`etc/config.properties`):
```properties
# Total memory a single query can consume across ALL workers combined
query.max-memory=500GB

# Maximum user memory a single query can consume on ANY SINGLE worker
query.max-memory-per-node=24GB

# Maximum total memory (user + system) a query can consume per worker
query.max-total-memory-per-node=32GB

# Spill to disk when queries exceed memory limits (if fault-tolerant mode)
spill-enabled=true
spiller-spill-path=/mnt/nvme-spill
```

---

## 2. Coordinator & Worker Resource Sizing

### Recommended Node Profiles:
- **Workers:**
  - **CPU:** 16–32 vCPUs (Trino thrives on fast single-thread performance and parallel cores).
  - **RAM:** 64GB–128GB RAM (allocate ~75–80% to JVM heap with `-Xmx`).
  - **JVM GC:** Use **G1GC** with `-XX:G1ReservePercent=15 -XX:InitiatingHeapOccupancyPercent=45`.
- **Coordinator:**
  - **CPU:** 8–16 vCPUs.
  - **RAM:** 32GB–64GB RAM (Coordinator handles query planning and metadata caching, not data processing).

---

## 3. Join Strategy Optimization & Dynamic Filtering

### Broadcast vs. Partitioned Join Tuning:
```properties
# Automatically reorders joins and picks broadcast vs. partitioned based on statistics
join-reordering-strategy=AUTOMATIC
join-distribution-type=AUTOMATIC
```

- When joining a large table with a small table, Trino broadcasts the small table to all workers, avoiding a full-table network shuffle.
- **Always run `ANALYZE table_name;`** on Iceberg/Hive tables to give Trino accurate row counts and column distinct values.

### Enabling Dynamic Filtering:
```properties
dynamic-filtering.enabled=true
dynamic-filtering.small-partitioned-profile=true
```

---

## 4. Connector-Level Pushdowns & Lakehouse File Sizing

### S3 / Iceberg Performance Tuning:
1. **Target Parquet File Size:** Maintain data file sizes between **128MB and 512MB**.
   - Too small (<10MB): Trino spends all its time doing HTTP GET metadata roundtrips on S3.
   - Too large (>2GB): Slow split generation and poor parallelization.
2. **Enable Native S3 File System Caching (RubiX / Trino FileSystem Cache):**
```properties
# etc/catalog/iceberg.properties
fs.cache.enabled=true
fs.cache.directories=/mnt/nvme-cache
fs.cache.max-allocation-size=64MB
fs.cache.ttl=2d
```
Local NVMe caching turns 50ms S3 read latency into sub-millisecond local disk reads.

---

## 5. Query Concurrency & Resource Groups

Prevent single runaway analytical queries from starving interactive executive dashboards using **Resource Groups** (`etc/resource-groups.json`):

```json
{
  "rootGroups": [
    {
      "name": "bi_dashboards",
      "maxQueuedQueries": 100,
      "hardConcurrencyLimit": 20,
      "hardCpuLimit": "1h",
      "schedulingPolicy": "weighted",
      "jmxExport": true
    },
    {
      "name": "adhoc_analytics",
      "maxQueuedQueries": 50,
      "hardConcurrencyLimit": 5,
      "hardCpuLimit": "30m"
    }
  ],
  "selectors": [
    { "source": "superset|tableau", "group": "bi_dashboards" },
    { "source": ".*", "group": "adhoc_analytics" }
  ]
}
```

---

## 6. Common Anti-Patterns & Pitfalls

| Anti-Pattern | Why It Fails | Solution |
| :--- | :--- | :--- |
| **`SELECT *` on 1,000-column tables** | Transfers gigabytes of unneeded data over S3 network; defeats columnar projection. | Explicitly select required columns. |
| **Joining without Table Statistics** | CBO cannot choose between Broadcast and Partitioned joins, defaulting to slow shuffles. | Regularly execute `ANALYZE table_name`. |
| **Querying millions of tiny files** | S3 request rate limits (HTTP 503) and high metadata overhead. | Run Iceberg compaction (`rewrite_data_files`). |
| **Cross Joins (`JOIN ON 1=1`)** | Generates Cartesian product ($N \times M$), causing immediate worker OOM. | Ensure explicit join keys exist on all joins. |
| **Treating Trino as an OLTP Database** | Trino is an analytical MPP engine; high-frequency single-row point writes degrade cluster. | Use Kafka/Flink for streaming writes into Iceberg. |

---

## 7. Production Monitoring & Telemetry

### Key Metrics to Monitor via JMX / Prometheus:
- **`trino.execution:name=QueryManager`**: `RunningQueries`, `QueuedQueries`, `FailedQueries`.
- **`trino.memory:name=MemoryPool`**: `FreeDistributedBytes`, `ReservedDistributedBytes`.
- **`trino.execution.executor:name=TaskExecutor`**: `BlockedSplits`, `RunningSplits`.

---

## 8. References & Further Reading

1. **Trino Tuning & Configuration:** [https://trino.io/docs/current/admin/properties.html](https://trino.io/docs/current/admin/properties.html)
2. **Trino Resource Groups:** [https://trino.io/docs/current/admin/resource-groups.html](https://trino.io/docs/current/admin/resource-groups.html)
3. **Trino Iceberg Performance Tuning:** [https://trino.io/docs/current/connector/iceberg.html#performance-tuning](https://trino.io/docs/current/connector/iceberg.html#performance-tuning)
