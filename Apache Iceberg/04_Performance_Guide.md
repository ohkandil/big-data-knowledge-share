# Apache Iceberg: Performance Tuning & Production Table Maintenance

## Table of Contents
1. [Target File Sizing & Compression Tuning](#1-target-file-sizing--compression-tuning)
2. [Automated Compaction: `rewrite_data_files`](#2-automated-compaction-rewrite_data_files)
3. [Manifest Compaction: `rewrite_manifests`](#3-manifest-compaction-rewrite_manifests)
4. [Snapshot Expiration & Orphan File Removal](#4-snapshot-expiration--orphan-file-removal)
5. [Mitigating Row-Level Delete File Degradation](#5-mitigating-row-level-delete-file-degradation)
6. [Sorting Strategies: Z-Order & Hierarchical Sorting](#6-sorting-strategies-z-order--hierarchical-sorting)
7. [Common Anti-Patterns & Pitfalls](#7-common-anti-patterns--pitfalls)
8. [Production Maintenance Automation Checklist](#8-production-maintenance-automation-checklist)
9. [References & Further Reading](#9-references--further-reading)

---

## 1. Target File Sizing & Compression Tuning

```sql
-- Optimal Table Properties for High-Performance Analytics
ALTER TABLE sales_db.orders SET TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.parquet.compression-level' = '7',
    'write.target-file-size-bytes' = '536870912', -- 512 MB target data file size
    'history.expire.max-snapshot-age-ms' = '604800000', -- 7 days retention
    'commit.retry.num-retries' = '10'
);
```

---

## 2. Automated Compaction: `rewrite_data_files`

High-frequency streaming ingestion produces thousands of small Parquet files. Run compaction regularly using Spark or Trino:

```sql
-- Spark SQL: Bin-pack small files into 512MB target files
CALL system.rewrite_data_files(
    table => 'sales_db.orders',
    strategy => 'binpack',
    options => map(
        'target-file-size-bytes', '536870912', -- 512 MB
        'min-input-files', '5'
    )
);
```

---

## 3. Manifest Compaction: `rewrite_manifests`

Frequent commits create thousands of small manifest files, slowing down query planning:

```sql
-- Spark SQL: Compact manifest files to optimize query planning speed
CALL system.rewrite_manifests('sales_db.orders');
```

---

## 4. Snapshot Expiration & Orphan File Removal

```sql
-- 1. Expire old snapshots older than 7 days (frees storage & metadata)
CALL system.expire_snapshots(
    table => 'sales_db.orders',
    older_than => TIMESTAMP '2025-01-08 00:00:00'
);

-- 2. Remove unreferenced orphan data files left by failed crashed jobs
CALL system.remove_orphan_files(
    table => 'sales_db.orders',
    older_than => TIMESTAMP '2025-01-12 00:00:00'
);
```

---

## 5. Mitigating Row-Level Delete File Degradation

In Merge-on-Read (MOR) tables, having more than 5 delete files per data file slows down queries. 
- Running `rewrite_data_files` automatically merges delete files back into clean, sequential Parquet data files.

---

## 6. Sorting Strategies: Z-Order & Hierarchical Sorting

For multi-column filtering acceleration on huge tables (billions of rows):

```sql
-- Spark SQL: Apply multidimensional Z-Ordering on customer_id and order_date
CALL system.rewrite_data_files(
    table => 'sales_db.orders',
    strategy => 'sort',
    sort_order => 'zorder(customer_id, order_date)'
);
```

---

## 7. Common Anti-Patterns & Pitfalls

| Anti-Pattern | Why It Fails | Solution |
| :--- | :--- | :--- |
| **Never running `expire_snapshots`** | S3 bucket cost explodes; metadata tree grows to gigabytes; query planning crawls. | Schedule daily `expire_snapshots` maintenance job. |
| **Committing every 2 seconds from Flink** | Creates hundreds of thousands of manifests and files daily. | Tune Flink checkpoint interval to 30–60 seconds. |
| **High volume of Equality Deletes without compaction** | MOR query scan performance degrades exponentially. | Run scheduled `rewrite_data_files` to squash deletes into data files. |
| **Partitioning on high-cardinality columns (e.g. `user_id`)** | Generates millions of tiny S3 partitions. | Partition by coarse ranges (`day(ts)`) or use `bucket(16, user_id)`. |

---

## 8. Production Maintenance Automation Checklist

- [ ] Daily automated **Data File Compaction** (`rewrite_data_files`).
- [ ] Weekly **Manifest Optimization** (`rewrite_manifests`).
- [ ] Daily **Snapshot Expiration** (`expire_snapshots` with 7-day retention).
- [ ] Weekly **Orphan File Cleanup** (`remove_orphan_files`).
- [ ] Table statistics gathered via `ANALYZE TABLE` for Trino CBO.

---

## 9. References & Further Reading

1. **Iceberg Table Maintenance Guide:** [https://iceberg.apache.org/docs/latest/maintenance/](https://iceberg.apache.org/docs/latest/maintenance/)
2. **Iceberg Spark Procedures:** [https://iceberg.apache.org/docs/latest/spark-procedures/](https://iceberg.apache.org/docs/latest/spark-procedures/)
3. **Z-Ordering in Iceberg:** [https://iceberg.apache.org/docs/latest/spark-writes/#sorting-strategies](https://iceberg.apache.org/docs/latest/spark-writes/#sorting-strategies)
