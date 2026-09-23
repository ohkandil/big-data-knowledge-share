# StarRocks: Data Processing, Table Models & Lakehouse Engine

## Table of Contents
1. [The Four Core Table Data Models](#1-the-four-core-table-data-models)
   - [Primary Key Model (Real-Time Upsert)](#primary-key-model-real-time-upsert)
   - [Duplicate Key Model (Append-Only Event Logs)](#duplicate-key-model-append-only-event-logs)
   - [Aggregate Key Model (Pre-Aggregated Metrics)](#aggregate-key-model-pre-aggregated-metrics)
   - [Unique Key Model (Legacy Deduplication)](#unique-key-model-legacy-deduplication)
2. [Data Partitioning & Bucketing Strategy](#2-data-partitioning--bucketing-strategy)
3. [Physical Storage Layout: Tablets & Columnar Encoding](#3-physical-storage-layout-tablets--columnar-encoding)
4. [External Catalogs: Iceberg, Paimon, Hudi, Hive](#4-external-catalogs-iceberg-paimon-hudi-hive)
5. [Data Ingestion Pipelines: Stream Load, Routine Load, Flink Connector](#5-data-ingestion-pipelines-stream-load-routine-load-flink-connector)
6. [Asynchronous Materialized Views (Async MVs)](#6-asynchronous-materialized-views-async-mvs)
7. [References & Further Reading](#7-references--further-reading)

---

## 1. The Four Core Table Data Models

StarRocks provides four distinct internal table models tailored for specific analytical patterns:

```
                          [ STARROCKS TABLE MODELS ]
                                      │
     ┌──────────────────┬─────────────┴────────────┬──────────────────┐
     ▼                  ▼                          ▼                  ▼
[ PRIMARY KEY ]    [ DUPLICATE KEY ]        [ AGGREGATE KEY ]    [ UNIQUE KEY ]
Real-time CDC      Append-only logs         Pre-computed rollups Legacy deduplication
(Delete Bitmaps)   (Raw clickstreams)       (SUM, MIN, MAX)      (Merge-on-Read / COW)
```

### Primary Key Model (Real-Time Upsert)
The **Primary Key Model** is the flagship engine for transactional synchronization (CDC from PostgreSQL/MySQL):
- **Delete Bitmap Mechanism:** StarRocks maintains an in-memory delete bitmap marking obsolete rows.
- **Query Performance:** Because delete bitmaps filter obsolete rows during scan time without performing expensive multi-version row merges, query latency is identical to append-only tables.

```sql
CREATE TABLE users_cdc (
    user_id BIGINT,
    user_name VARCHAR(100),
    email VARCHAR(200),
    updated_at DATETIME
) ENGINE=OLAP
PRIMARY KEY(user_id)
PARTITION BY date_trunc('month', updated_at)
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "enable_persistent_index" = "true"
);
```

### Duplicate Key Model (Append-Only Event Logs)
Optimized for immutable events (IoT telemetry, clickstreams, audit logs).
- Data is stored exactly as inserted, sorted by the duplicate key for fast range filtering.

### Aggregate Key Model (Pre-Aggregated Metrics)
Automatically rolls up rows sharing identical dimension keys during ingestion and background compactions:
```sql
CREATE TABLE site_traffic (
    site_id INT,
    event_date DATE,
    page_views BIGINT SUM DEFAULT "0",
    unique_visitors BITMAP BITMAP_UNION
) ENGINE=OLAP
AGGREGATE KEY(site_id, event_date)
DISTRIBUTED BY HASH(site_id) BUCKETS 8;
```

---

## 2. Data Partitioning & Bucketing Strategy

StarRocks organizes tables into a two-level physical hierarchy: **Partitioning** and **Bucketing**.

```
[ Table: orders ]
       │
       ├─► Partition: 2025-01 (Time-based range: date_trunc('day', order_date))
       │         │
       │         ├─► Bucket 0 (Tablet 10001): HASH(order_id) % 4
       │         ├─► Bucket 1 (Tablet 10002): HASH(order_id) % 4
       │         ├─► Bucket 2 (Tablet 10003): HASH(order_id) % 4
       │         └─► Bucket 3 (Tablet 10004): HASH(order_id) % 4
       │
       └─► Partition: 2025-02
```

- **Partitioning (Level 1):** Range or List partitioning (typically by Date/Timestamp). Enables partition pruning to skip entire time ranges.
- **Bucketing (Level 2):** Hash partitioning by key (e.g., `user_id`, `order_id`). Each bucket maps to a physical **Tablet** replicated across Backend (BE) nodes.

---

## 3. Physical Storage Layout: Tablets & Columnar Encoding

Inside each Backend node, a Tablet is stored as columnar data files containing:
- **Segment Files:** Columnar-formatted binary data chunks (default 256MB).
- **Zone Maps:** Stores `min`, `max`, and `null_count` for every column block for zero-cost row skipping.
- **Bloom Filters:** Optional probabilistic filters on high-cardinality columns (e.g., UUIDs) to avoid reading non-matching blocks.
- **Bitmap Indexes:** Inverted bitmap indexes accelerating ad-hoc multi-column slice-and-dice queries.

---

## 4. External Catalogs: Iceberg, Paimon, Hudi, Hive

StarRocks can query external Lakehouse tables directly **without loading data into internal storage**:

```sql
-- Create an external Iceberg Catalog pointing to S3 REST catalog
CREATE EXTERNAL CATALOG iceberg_lake
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "https://catalog.internal:8443",
    "aws.s3.region" = "us-east-1",
    "aws.s3.enable_ssl" = "true"
);

-- Directly query the external lakehouse table with vectorized speed
SELECT 
    customer_id,
    SUM(total_amount) AS revenue
FROM iceberg_lake.sales_db.orders
WHERE order_date >= '2025-01-01'
GROUP BY 1;
```

---

## 5. Data Ingestion Pipelines

| Ingestion Method | Mechanism | Latency Profile | Best For |
| :--- | :--- | :--- | :--- |
| **Stream Load** | Synchronous HTTP PUT pushing CSV, JSON, or Parquet | Sub-second | Custom microservice pipelines |
| **Routine Load** | Built-in continuous consumer subscribing to Apache Kafka | Real-time (<100ms) | Kafka topic ingestion |
| **Flink-StarRocks-Connector**| High-throughput vectorized 2PC streaming sink | Exactly-Once (10s) | Flink streaming ETL output |
| **Broker Load / S3 Load** | Asynchronous bulk pull from S3, HDFS, or ADLS | Batch (Minutes) | Initial historical data backfills |

---

## 6. Asynchronous Materialized Views (Async MVs)

StarRocks Async MVs can be created over **both internal tables and external Lakehouse tables**:

```sql
CREATE MATERIALIZED VIEW mv_daily_sales
REFRESH ASYNC EVERY(INTERVAL 1 HOUR)
PARTITION BY order_date
DISTRIBUTED BY HASH(store_id) BUCKETS 8
AS 
SELECT 
    order_date,
    store_id,
    COUNT(order_id) AS total_orders,
    SUM(amount) AS total_revenue
FROM iceberg_lake.sales.orders
GROUP BY 1, 2;
```

### Transparent Query Rewrite:
When a user runs a query against the raw `iceberg_lake.sales.orders` table, the StarRocks CBO automatically intercepts the query and rewrites the plan to scan `mv_daily_sales`, executing in 5 milliseconds instead of 5 seconds.

---

## 7. References & Further Reading

1. **StarRocks Table Models:** [https://docs.starrocks.io/docs/table_design/table_types/](https://docs.starrocks.io/docs/table_design/table_types/)
2. **StarRocks External Catalogs:** [https://docs.starrocks.io/docs/data_source/catalog/](https://docs.starrocks.io/docs/data_source/catalog/)
3. **StarRocks Materialized Views:** [https://docs.starrocks.io/docs/using_starrocks/async_mv/](https://docs.starrocks.io/docs/using_starrocks/async_mv/)
