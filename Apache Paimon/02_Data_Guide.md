# Apache Paimon: Data Engine & Processing Mechanics

## Table of Contents
1. [LSM-Tree on Cloud Object Storage](#1-lsm-tree-on-cloud-object-storage)
2. [Table Types: Primary Key vs. Append-Only](#2-table-types-primary-key-vs-append-only)
3. [Merge Engines: Deduplicate, Partial-Update, Aggregation](#3-merge-engines-deduplicate-partial-update-aggregation)
4. [Bucketing Strategies: Fixed vs. Dynamic Bucketing](#4-bucketing-strategies-fixed-vs-dynamic-bucketing)
5. [Changelog Producers: Input, Lookup, Full-Compaction](#5-changelog-producers-input-lookup-full-compaction)
6. [Deletion Vectors & Compaction Lifecycle](#6-deletion-vectors--compaction-lifecycle)
7. [References & Further Reading](#7-references--further-reading)

---

## 1. LSM-Tree on Cloud Object Storage

Apache Paimon structures data within each bucket as a **multi-level LSM-tree**:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     PAIMON BUCKET LSM STORAGE STRUCTURE                 │
├─────────────────────────────────────────────────────────────────────────┤
│  [ In-Memory MemTable (TaskHeap) ] ──► Sorted buffer                    │
│                 │                                                       │
│                 ▼ Flush                                                 │
│  [ Level 0 (L0) ]: [ data-001.parquet ] [ data-002.parquet ] (Unmerged) │
│                 │                                                       │
│                 ▼ Minor Compaction                                      │
│  [ Level 1 (L1) ]: [ data-101.parquet ] [ data-102.parquet ] (Sorted)   │
│                 │                                                       │
│                 ▼ Major Compaction                                      │
│  [ Level 2 (L2) ]: [ data-201.parquet ] [ data-202.parquet ] (Compacted)│
└─────────────────────────────────────────────────────────────────────────┘
```

- **Level 0 ($L_0$):** Flushed directly from memory; files can have overlapping primary key ranges.
- **Levels 1+ ($L_1, L_2 \dots$):** Merged and sorted by primary key range. No two files in the same level share overlapping keys.

---

## 2. Table Types: Primary Key vs. Append-Only

### 1. Primary Key Table
```sql
CREATE TABLE paimon_catalog.default.orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_status VARCHAR(50),
    amount DECIMAL(10, 2),
    order_time TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'changelog-producer' = 'lookup',
    'merge-engine' = 'deduplicate'
);
```

### 2. Append-Only Table
```sql
CREATE TABLE paimon_catalog.default.server_logs (
    server_id VARCHAR(50),
    log_level VARCHAR(10),
    message STRING,
    log_time TIMESTAMP(3)
) WITH (
    'bucket' = '-1', -- Unaware bucket mode (dynamic parallel appends)
    'write-mode' = 'append-only'
);
```

---

## 3. Merge Engines: Deduplicate, Partial-Update, Aggregation

### 1. Deduplicate (Default)
Retains only the latest row for each primary key based on sequence number or arrival order.

### 2. Partial-Update
Enables multiple upstream streaming jobs to update separate columns of the same row:
```sql
-- Pipeline A writes: (user_id=1, email='alice@mail.com', phone=NULL)
-- Pipeline B writes: (user_id=1, email=NULL, phone='+1-555-0100')
-- Paimon Merged Result: (user_id=1, email='alice@mail.com', phone='+1-555-0100')
```

### 3. Aggregation Engine
Pre-aggregates metric columns directly inside storage files:
```sql
CREATE TABLE product_daily_stats (
    product_id BIGINT,
    stat_date DATE,
    views BIGINT,
    revenue DECIMAL(12, 2),
    PRIMARY KEY (product_id, stat_date) NOT ENFORCED
) WITH (
    'merge-engine' = 'aggregation',
    'fields.views.aggregate-function' = 'sum',
    'fields.revenue.aggregate-function' = 'sum'
);
```

---

## 4. Bucketing Strategies: Fixed vs. Dynamic Bucketing

- **Fixed Bucketing (`'bucket' = 'N'`):** Partitions data across $N$ deterministic buckets using `HASH(primary_key) % N`.
- **Dynamic Bucketing (`'bucket' = '-1'`):** For primary key tables, Paimon dynamically allocates buckets as data grows, avoiding manual bucket tuning.

---

## 5. Changelog Producers: Input, Lookup, Full-Compaction

| Changelog Mode | Mechanism | Latency | Overhead |
| :--- | :--- | :--- | :--- |
| **`none`** | Emits no changelog (for pure batch reads). | N/A | Lowest |
| **`input`** | Relies on input stream already containing $-U / +U$ CDC events. | Sub-second | Minimal |
| **`lookup`** | Checks existing keys on local RocksDB SSD cache during write to produce $-U$ and $+U$. | Sub-minute | Moderate CPU/SSD |
| **`full-compaction`** | Generates changelog deltas during asynchronous LSM compaction runs. | Minutes | Lowest write overhead |

---

## 6. Deletion Vectors & Compaction Lifecycle

- **Deletion Vectors:** Uses Roaring Bitmaps to record deleted row positions in $L_1/L_2$ files, avoiding immediate file rewriting.
- **Dedicated Compaction Jobs:** In high-throughput streaming pipelines, compaction can be offloaded to a separate Flink job (`flink run ... paimon-compaction`), ensuring streaming writer tasks never suffer backpressure.

---

## 7. References & Further Reading

1. **Paimon Primary Key Tables:** [https://paimon.apache.org/docs/master/primary-key-table/overview/](https://paimon.apache.org/docs/master/primary-key-table/overview/)
2. **Paimon Merge Engines:** [https://paimon.apache.org/docs/master/primary-key-table/merge-engine/](https://paimon.apache.org/docs/master/primary-key-table/merge-engine/)
3. **Paimon Changelog Producers:** [https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/)
