# Apache Iceberg: Architecture, Catalogs & Commit Protocol

## Table of Contents
1. [The Catalog Layer: Standardized REST, Glue, Nessie, HMS](#1-the-catalog-layer-standardized-rest-glue-nessie-hms)
2. [The Metadata Tree Architecture](#2-the-metadata-tree-architecture)
3. [The Atomic Commit Protocol & Optimistic Concurrency Control](#3-the-atomic-commit-protocol--optimistic-concurrency-control)
4. [Streaming Ingestion with Apache Flink & Spark](#4-streaming-ingestion-with-apache-flink--spark)
5. [Scan Planning Architecture: Manifest & Partition Pruning](#5-scan-planning-architecture-manifest--partition-pruning)
6. [References & Further Reading](#6-references--further-reading)

---

## 1. The Catalog Layer

The **Catalog** is the single entry point for any query engine interacting with an Iceberg table. Its sole responsibility is to atomically store and update the pointer to the latest **`vN.metadata.json`** file.

| Catalog Type | Mechanism | Best For |
| :--- | :--- | :--- |
| **REST Catalog** | Open OpenAPI REST specification; cloud-native HTTP service | Enterprise standard (Polaris, Tabular, Unity, AWS Glue) |
| **Project Nessie** | Git-like branching (`main`, `dev_branch`), tagging, and merging | Data CI/CD, A/B testing, isolated multi-table staging |
| **AWS Glue** | AWS-managed database catalog using native IAM authentication | AWS cloud-native serverless deployments |
| **Hive Metastore (HMS)**| Uses HMS Thrift service storing table metadata in MySQL/Postgres | Legacy Hadoop migration environments |
| **JDBC Catalog** | Uses relational database (Postgres/MySQL) locking | Small private Kubernetes clusters |

---

## 2. The Metadata Tree Architecture

```
                                [ CATALOG POINTER ]
                                         │
                                         ▼
                      ┌────────────────────────────────────┐
                      │        v4.metadata.json            │
                      │ • Schema (Field IDs 1..N)          │
                      │ • Partition Spec (v1: month, v2:day│
                      │ • Current Snapshot: ID 9002        │
                      └──────────────────┬─────────────────┘
                                         │ Points to
                                         ▼
                      ┌────────────────────────────────────┐
                      │    snap-9002.manifest-list.avro    │
                      │ • Manifest 1: (created_at=2025-01) │
                      │ • Manifest 2: (created_at=2025-02) │
                      └────────┬───────────────────┬───────┘
                               │                   │
                ┌──────────────┘                   └──────────────┐
                ▼                                                 ▼
   ┌───────────────────────────┐                     ┌───────────────────────────┐
   │      manifest-1.avro      │                     │      manifest-2.avro      │
   │ • data-01.parquet         │                     │ • data-03.parquet         │
   │   (lower=100, upper=5000) │                     │   (lower=5001, upper=9900)│
   │ • data-02.parquet         │                     │ • delete-01.parquet       │
   └────────────┬──────────────┘                     └────────────┬──────────────┘
                │                                                 │
                ▼                                                 ▼
    [ S3: data-01.parquet ]                              [ S3: data-03.parquet ]
```

---

## 3. The Atomic Commit Protocol & Optimistic Concurrency Control

```
   [ Writer A (Flink) ]                                   [ Writer B (Spark) ]
          │                                                       │
          ├─► 1. Reads current Snapshot S1                        ├─► 1. Reads current Snapshot S1
          ├─► 2. Writes data files to S3                          ├─► 2. Writes data files to S3
          ├─► 3. Creates Snapshot S2 (refs new files + S1)        ├─► 3. Creates Snapshot S2'
          ▼                                                       ▼
   [ ATOMIC COMMIT TO CATALOG ]                           [ ATOMIC COMMIT TO CATALOG ]
   (CAS: Swap S1 -> S2 succeeds!)                         (CAS: Swap S1 -> S2' fails! Current is S2)
                                                                  │
                                                                  ▼ (Conflict Detection)
                                                          Validates file intersection:
                                                          No overlapping partitions?
                                                          Rebases S2' -> S3 and commits!
```

---

## 4. Streaming Ingestion with Apache Flink & Spark

- **Flink Checkpoint Committer:** Flink's `IcebergFilesCommitter` operator executes during checkpoint cycles, converting completed data files into a single atomic Iceberg snapshot commit every 30–60 seconds.
- **Spark Structured Streaming:** Micro-batch streaming sink appending Parquet data files on micro-batch triggers.

---

## 5. Scan Planning Architecture: Manifest & Partition Pruning

Query planning executes in 3 rapid phases **without touching data files**:
1. **Manifest List Pruning:** Trino/Spark reads `snap-N.manifest-list.avro`, evaluating query partition filters against each manifest's partition range summary to eliminate non-matching manifests.
2. **Manifest File Scanning:** Reads surviving manifest files, evaluating column min/max stats (`lower_bounds`, `upper_bounds`) against query `WHERE` predicates.
3. **Split Generation:** Returns the minimal list of qualifying Parquet file paths to workers for distributed scanning.

---

## 6. References & Further Reading

1. **Iceberg Catalog Specification:** [https://iceberg.apache.org/spec/#catalog-specification](https://iceberg.apache.org/spec/#catalog-specification)
2. **Iceberg REST Open API Spec:** [https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
3. **Iceberg Concurrency & Transactions:** [https://iceberg.apache.org/docs/latest/reliability/](https://iceberg.apache.org/docs/latest/reliability/)
