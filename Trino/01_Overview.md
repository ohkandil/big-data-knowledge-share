# Trino: Architectural Overview & Foundational Concepts

```
   ████████╗██████╗ ██╗███╗   ██╗ ██████╗ 
   ╚══██╔══╝██╔══██╗██║████╗  ██║██╔═══██╗
      ██║   ██████╔╝██║██╔██╗ ██║██║   ██║
      ██║   ██╔══██╗██║██║╚██╗██║██║   ██║
      ██║   ██║  ██║██║██║ ╚████║╚██████╔╝
      ╚═╝   ╚═╝  ╚═╝╚═╝╚═╝  ╚═══╝ ╚═════╝ 
   Fast Distributed SQL Query Engine for Big Data
```

---

## Table of Contents
1. [Executive Summary & Core Mission](#1-executive-summary--core-mission)
2. [Historical Context & Genesis: Why Trino Was Created](#2-historical-context--genesis-why-trino-was-created)
   - [Facebook's 300PB Data Warehouse Challenge](#facebooks-300pb-data-warehouse-challenge)
   - [The Presto to Trino Rebranding](#the-presto-to-trino-rebranding)
   - [Separation of Storage and Compute](#separation-of-storage-and-compute)
3. [The Modern Streamhouse / Lakehouse Ecosystem Context](#3-the-modern-streamhouse--lakehouse-ecosystem-context)
   - [Positioning Trino as the Unified Interactive Query Layer](#positioning-trino-as-the-unified-interactive-query-layer)
   - [Trino with Iceberg, Delta Lake, Hive, and Kafka](#trino-with-iceberg-delta-lake-hive-and-kafka)
4. [High-Level Architectural Topology](#4-high-level-architectural-topology)
   - [Coordinator vs. Worker Roles](#coordinator-vs-worker-roles)
   - [The Connector Architecture (Storage Plugin Model)](#the-connector-architecture-storage-plugin-model)
5. [How Trino Processes Data at a High Level](#5-how-trino-processes-data-at-a-high-level)
   - [In-Memory Streaming Execution Pipeline](#in-memory-streaming-execution-pipeline)
   - [Query Federation & Cross-Source Joins](#query-federation--cross-source-joins)
   - [Cost-Based Optimizer (CBO)](#cost-based-optimizer-cbo)
6. [Trino vs. Alternative Query Engines](#6-trino-vs-alternative-query-engines)
7. [Key Terminology & Mental Model Glossary](#7-key-terminology--mental-model-glossary)
8. [References & Further Reading](#8-references--further-reading)

---

## 1. Executive Summary & Core Mission

**Trino** (formerly **PrestoSQL**) is an open-source, distributed, Massively Parallel Processing (MPP) SQL query engine designed for **fast, interactive ad-hoc analytics and federated queries over petabyte-scale data lakes and disparate data stores**.

Unlike traditional relational databases or cloud data warehouses (Snowflake, BigQuery) that bundle storage format, catalog, and query execution together, Trino is **pure compute**:
- It does **not** own or store data.
- It executes standard ANSI SQL queries directly against underlying storage systems (Amazon S3, Azure ADLS, Google Cloud Storage, HDFS, PostgreSQL, MySQL, Apache Kafka, Elasticsearch, Cassandra).
- It allows data engineers and analysts to query open lakehouse formats (Apache Iceberg, Delta Lake, Apache Hive, Apache Hudi) and join data across completely different physical databases in a single SQL statement.

### Primary Capabilities:
- **Sub-Second to Interactive Latency:** Pipelined in-memory streaming engine without intermediate disk spill bottlenecks for ad-hoc exploration and BI dashboards.
- **ANSI SQL Compliance:** Full support for complex SQL features: window functions, recursive CTEs, lambda expressions, geospatial queries, and JSON manipulation.
- **Pluggable Connector Architecture:** Standardized Service Provider Interface (SPI) enabling query access to dozens of data sources.
- **Cross-Source Query Federation:** Join an Apache Iceberg table on S3 with a PostgreSQL customer table and a real-time Apache Kafka topic in one SQL query.
- **Cost-Based Optimizer (CBO):** Advanced join reordering, dynamic filtering, partition pruning, and pushdown predicate evaluations.

```
                               ┌────────────────────────────────────────────────────────┐
                               │                      TRINO ENGINE                      │
                               │            (Coordinator + Worker Cluster)              │
                               └───────┬───────────────────┬────────────────────┬───────┘
                                       │                   │                    │
              ┌────────────────────────┴─────────┐         │         ┌──────────┴────────────────────────┐
              ▼                                  ▼         ▼         ▼                                   ▼
    [ Iceberg on S3 / ADLS ]           [ PostgreSQL / MySQL ]   [ Kafka Stream ]                [ Elasticsearch ]
    (Multi-PB Lakehouse Analytics)     (Live Dimension Tables)  (Real-Time Ingestion)           (Search & Logs)
```

---

## 2. Historical Context & Genesis: Why Trino Was Created

### Facebook's 300PB Data Warehouse Challenge

In 2012, Facebook operated the world's largest Apache Hadoop data warehouse (over 300 petabytes across thousands of nodes). Data analysts, scientists, and engineers queried this warehouse using **Apache Hive**:
- Hive compiled SQL into Apache MapReduce jobs.
- MapReduce wrote every intermediate stage's output to HDFS disks.
- Even simple aggregation queries (`SELECT COUNT(*)`) took 5 to 30 minutes to execute.

To enable interactive, human-speed SQL exploration, Martin Traverso, Dain Sundstrom, David Phillips, and Eric Hwang created **Presto** at Facebook in 2012. Presto delivered 10x–50x performance improvements by:
1. Executing queries entirely in-memory using pipelined operators.
2. Replacing MapReduce disk shuffles with in-memory HTTP/TCP data exchange buffers.
3. Decoupling the query engine from HDFS via pluggable storage connectors.

### The Presto to Trino Rebranding

In 2018, the original creators left Facebook to preserve an open, community-driven governance model, launching the **PrestoSQL** fork. In December 2020, to eliminate trademark confusion with Facebook's PrestoDB, the project was officially renamed **Trino** (backed by the Trino Software Foundation).

---

## 3. The Modern Streamhouse / Lakehouse Ecosystem Context

### Positioning Trino as the Unified Interactive Query Layer

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         ENTERPRISE LAKEHOUSE / STREAMHOUSE                               │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│  DATA INGESTION & CONTINUOUS LOGISTICS                                                   │
│  Kafka / NiFi / Flink CDC ──► [ Open Lakehouse Storage: S3 / ADLS / GCS ]                │
│                               • Apache Iceberg / Apache Paimon / Delta Lake              │
│                                                                                          │
│                                           │                                              │
│                                           ▼                                              │
│  INTERACTIVE SQL QUERY & FEDERATION LAYER                                                │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                    TRINO                                           │  │
│  │  • Multi-PB Ad-hoc Data Lake Exploration                                           │  │
│  │  • Interactive BI & Dashboard Acceleration (Superset, Tableau, PowerBI)            │  │
│  │  • Cross-Engine Query Federation (Join Lakehouse + Postgres + MongoDB)             │  │
│  │  • Large-scale Batch Transformations via dbt-trino (ETL / Mart Generation)         │  │
│  └────────────────────────────────────┬───────────────────────────────────────────────┘  │
│                                       │                                                  │
│                                       ▼                                                  │
│  [ CONSUMERS ]                                                                           │
│  Data Scientists (Notebooks) · Business Analysts · Automated dbt Jobs · REST APIs        │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. High-Level Architectural Topology

Trino follows a **Master-Worker MPP architecture** running on the JVM.

```
                             ┌───────────────────────────────┐
                             │       CLIENT / BI / DBT       │
                             │ (JDBC, ODBC, Python, REST API)│
                             └───────────────┬───────────────┘
                                             │ HTTP/JSON Query Submission
                                             ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   TRINO COORDINATOR                                     │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐  │
│  │ SQL Parser &     │  │ Cost-Based       │  │ Execution Plan   │  │ Task Scheduler  │  │
│  │ Analyzer (ANTLR) │  │ Optimizer (CBO)  │  │ Generator (DAG)  │  │ & Split Manager │  │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘  └─────────────────┘  │
└───────────────────────┬─────────────────────────────┬───────────────────────────────────┘
                        │ Task Scheduling / Splits    │ Task Scheduling / Splits
                        ▼                             ▼
┌───────────────────────────────────────────┐ ┌───────────────────────────────────────────┐
│              TRINO WORKER 1               │ │              TRINO WORKER 2               │
│  ┌─────────────────────────────────────┐  │ │  ┌─────────────────────────────────────┐  │
│  │ Driver / Pipeline / Task Engine     │  │ │  │ Driver / Pipeline / Task Engine     │  │
│  │  • FilterAndProjectOperators        │  │ │  │  • FilterAndProjectOperators        │  │
│  │  • In-Memory Hash Join / GroupBy    │  │ │  │  • In-Memory Hash Join / GroupBy    │  │
│  └──────────────────┬──────────────────┘  │ │  └──────────────────┬──────────────────┘  │
│                     │ ExchangeClient      │ │                     │ ExchangeClient      │
│                     ▼                     │ │                     ▼                     │
│  ┌─────────────────────────────────────┐  │ │  ┌─────────────────────────────────────┐  │
│  │ Connector Plugin (Iceberg / Hive)   │  │ │  │ Connector Plugin (Iceberg / Hive)   │  │
│  └──────────────────┬──────────────────┘  │ │  └──────────────────┬──────────────────┘  │
└─────────────────────┼─────────────────────┘ └─────────────────────┼─────────────────────┘
                      │                                             │
                      ▼                                             ▼
       [ Object Storage: S3 / ADLS (Parquet Data) ]   [ Catalog: Iceberg REST / Hive Metastore ]
```

### Components:
1. **Coordinator:**
   - Single dedicated master node (or high-availability active-standby).
   - Parses incoming SQL statements, performs semantic analysis, runs the Cost-Based Optimizer (CBO), converts the logical plan into a distributed physical execution plan, and schedules tasks across workers.
2. **Workers:**
   - Daemon processes executing the physical tasks assigned by the Coordinator.
   - Workers fetch data chunks (**Splits**) directly from storage connectors, process rows in vectorized memory blocks (**Pages**), evaluate operators (joins, aggregations), and stream intermediate results to other workers via in-memory HTTP/TCP Exchanges.

---

## 5. How Trino Processes Data at a High Level

### In-Memory Streaming Execution Pipeline

Trino executes queries without writing intermediate stage results to disk:
1. **Splits:** When a query targets an Iceberg or Hive table, the connector divides the data files into logical chunks called **Splits** (typically 64MB–128MB).
2. **Pages & Blocks:** Data is read into columnar in-memory structures called **Pages** (a collection of columnar **Blocks** representing up to thousands of rows).
3. **Pipelined Streaming:** As soon as an operator (e.g., TableScan) produces a Page, downstream operators (e.g., Filter, HashJoin) consume it immediately in the same memory buffer, achieving minimal memory footprint and near-zero latency.

### Query Federation & Cross-Source Joins

Trino organizes data under a 3-level hierarchy: `catalog.schema.table`.

```sql
-- Federated Join across S3 Data Lake and PostgreSQL
SELECT 
    c.customer_name,
    c.country,
    SUM(o.amount_usd) AS total_spent
FROM iceberg_lake.sales.orders o       -- S3 Iceberg Table (Petabytes)
JOIN postgres_prod.public.customers c  -- Live PostgreSQL RDS (Megabytes)
    ON o.customer_id = c.customer_id
WHERE o.order_date >= DATE '2025-01-01'
GROUP BY 1, 2;
```

Trino's Coordinator distributes the Iceberg table scan across hundreds of workers, pushes the PostgreSQL scan down to the database connector via JDBC, and executes a distributed in-memory **Hash Join** across the workers.

---

## 6. Trino vs. Alternative Query Engines

| Architectural Dimension | Trino | StarRocks | Apache Spark (Spark SQL) | Snowflake |
| :--- | :--- | :--- | :--- | :--- |
| **Engine Core Language** | Java (JVM with Bytecode Compilation) | C++ (SIMD Vectorized Engine) | Scala / Java (JVM Catalyst Optimizer) | C++ (Proprietary Cloud Engine) |
| **Storage Model** | Pure Compute (Storage Decoupled / External) | Hybrid: Internal Primary-Key Storage + Lakehouse Querying | Decoupled / In-Memory Cache (RDDs) | Proprietary Micro-Partitions on Cloud Object Store |
| **Query Latency Profile** | Interactive (100ms – 10s) | Sub-Second / Real-Time OLAP (10ms – 2s) | Batch / Interactive (Seconds to Hours) | Interactive / Data Warehousing |
| **Query Federation** | Excellent (30+ native connectors) | Good (External Catalogs for Iceberg, Hive, Paimon) | Moderate (JDBC / Spark Data Sources) | Limited (External Tables) |
| **Fault Tolerance Strategy** | Pipelined (fast fail) or Task-Level Retry (Exchange Spooling) | MPP Pipelined execution | Resilient RDD DAG lineage / Task Retries | Managed Cloud Redundancy |
| **Primary Sweet Spot** | Ad-hoc data lake exploration, BI, federated SQL queries | High-concurrency real-time dashboards & metrics | Large-scale heavy ETL, ML, Unstructured Data | Enterprise cloud data warehousing |

---

## 7. Key Terminology & Mental Model Glossary

- **Coordinator:** The master node that parses, plans, optimizes, and coordinates query execution.
- **Worker:** A cluster node that executes assigned tasks and processes data.
- **Catalog:** Top-level namespace mapping to a specific connector and data source (e.g., `iceberg`, `hive`, `postgres`).
- **Schema:** Database / schema namespace within a catalog.
- **Table:** A collection of named typed columns and rows.
- **Connector:** The storage plugin (SPI) implementing metadata, split generation, and data access for a target system.
- **Split:** A discrete, parallelizable unit of data read from a connector (e.g., a byte range within a Parquet file).
- **Page:** The columnar in-memory data unit passed between operators (up to 1MB or ~16,000 rows).
- **Block:** A single typed columnar array inside a Page.
- **Exchange:** The operator responsible for transferring Pages between different workers and stages over the network.
- **Dynamic Filtering:** An optimization where runtime hash join keys are dynamically broadcast to table scans to eliminate reading irrelevant parquet files from storage.

---

## 8. References & Further Reading

1. **Official Trino Documentation:** [https://trino.io/docs/current/](https://trino.io/docs/current/)
2. **Trino: The Definitive Guide (Book):** Fuller, M., Moser, M., & Traverso, M. (O'Reilly Media, 2nd Edition, 2021).
3. **The Original Presto Paper:** Sethi, R., et al. (2019). *"Presto: SQL on Everything."* Proceedings of the 2019 International Conference on Management of Data (SIGMOD '19).
4. **Trino Architecture Overview:** [https://trino.io/docs/current/overview/concepts.html](https://trino.io/docs/current/overview/concepts.html)
