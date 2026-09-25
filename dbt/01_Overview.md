---
title: "dbt (data build tool): Overview & Foundational Concepts"
type: overview
tags:
  - dbt
  - sql-transformation
  - data-modeling
  - analytics-engineering
  - dag
  - 01-overview
aliases:
  - "dbt"
  - "data build tool"
  - "dbt Overview"
layer: "SQL Transformation & Modeling"
parent: "[[MOCs/MOC_Transformation_and_OLAP_Serving]]"
---

# dbt (data build tool): Architectural Overview & Foundational Concepts

```
   ██████╗ ██████╗ ████████╗
   ██╔══██╗██╔══██╗╚══██╔══╝
   ██║  ██║██████╔╝   ██║   
   ██║  ██║██╔══██╗   ██║   
   ██████╔╝██████╔╝   ██║   
   ╚═════╝ ╚═════╝    ╚═╝   
   Transform Data in Your Warehouse
```

---

## Table of Contents
1. [Executive Summary & Core Mission](#1-executive-summary--core-mission)
2. [Historical Context & Genesis: Why dbt Was Created](#2-historical-context--genesis-why-dbt-was-created)
   - [The Transition from ETL to ELT](#the-transition-from-etl-to-elt)
   - [Fishtown Analytics & Software Engineering for Data](#fishtown-analytics--software-engineering-for-data)
   - [The Declarative Modeling Paradigm](#the-declarative-modeling-paradigm)
3. [The Modern Streamhouse / Lakehouse Ecosystem Context](#3-the-modern-streamhouse--lakehouse-ecosystem-context)
   - [Positioning dbt in the Data-Lake-Stream-House Stack](#positioning-dbt-in-the-data-lake-stream-house-stack)
   - [dbt with Trino, StarRocks, Snowflake, BigQuery, and Databricks](#dbt-with-trino-starrocks-snowflake-bigquery-and-databricks)
4. [High-Level Architectural Topology](#4-high-level-architectural-topology)
   - [Compilation & Execution Graph (DAG)](#compilation--execution-graph-dag)
   - [The Role of Jinja & Macros](#the-role-of-jinja--macros)
5. [How dbt Processes Data at a High Level](#5-how-dbt-processes-data-at-a-high-level)
   - [Code Abstractions: Models, Sources, Seeds, Snapshots](#code-abstractions-models-sources-seeds-snapshots)
   - [Materialization Strategies: View, Table, Incremental, Ephemeral](#materialization-strategies-view-table-incremental-ephemeral)
   - [Testing, Documentation, and Lineage](#testing-documentation-and-lineage)
6. [dbt vs. Alternative Transformation Approaches](#6-dbt-vs-alternative-transformation-approaches)
7. [Key Terminology & Mental Model Glossary](#7-key-terminology--mental-model-glossary)
8. [References & Further Reading](#8-references--further-reading)

---

## 1. Executive Summary & Core Mission

**dbt (data build tool)** is an open-source data transformation framework that brings **software engineering best practices**—version control, testing, continuous integration, modularity, and documentation—to the data transformation ("T" in ELT) process.

Unlike traditional ETL tools (like Informatica, Talend, or SSIS) that pull data out of databases, transform it on separate compute servers, and load it back, dbt operates directly **inside your cloud data warehouse, lakehouse, or MPP query engine** (Snowflake, BigQuery, Databricks, Trino, StarRocks, PostgreSQL). dbt translates modular `SELECT` statements parameterized with Jinja into optimized DDL/DML statements, orchestrates their execution as a Directed Acyclic Graph (DAG), runs automated data quality tests, and generates interactive data catalogs.

### Primary Capabilities:
- **Pure SQL/Python Data Modeling:** Write simple `SELECT` queries; dbt handles boilerplate table creation, updates, and schema migrations.
- **Topological DAG Dependency Resolution:** Automatically infers dependencies between models via the `{{ ref('model_name') }}` function.
- **Automated Data Quality Testing:** Built-in assertions (`unique`, `not_null`, `accepted_values`, `relationships`) and custom SQL tests.
- **Version-Controlled Analytics Code:** Enables Git collaboration, branch previews, code reviews, and automated CI/CD pipelines for data teams.
- **Auto-Generated Documentation & Lineage:** Generates visual DAG dependency graphs and searchable column-level data dictionaries directly from model code and YAML definitions.

```
   [ Extract & Load ] ──► [ RAW DATA LAYER ] ──► ┌──────────────────────────────────────┐ ──► [ BUSINESS CONSUMERS ]
   • Fivetran / Airbyte    (Bronze Tables in       │               DBT DAG                │     • BI Dashboards (Tableau)
   • Kafka / Flink Sinks   Snowflake, BigQuery,    │  ┌───────────┐      ┌─────────────┐  │     • ML Feature Stores
   • NiFi Ingestion        Trino / Iceberg)        │  │ Staging   │ ──►  │ Intermediate│  │     • Reverse ETL (Census)
                                                   │  └───────────┘      └──────┬──────┘  │
                                                   │                            ▼         │
                                                   │                     ┌─────────────┐  │
                                                   │                     │    Marts    │  │
                                                   │                     └─────────────┘  │
                                                   └──────────────────────────────────────┘
```

---

## 2. Historical Context & Genesis: Why dbt Was Created

### The Transition from ETL to ELT

In legacy data warehousing architectures (1990s–2010s):
- Compute and storage inside relational databases (Oracle, Teradata, SQL Server) were expensive and non-elastic.
- Data had to be transformed *before* loading (ETL) using external processing engines (Informatica, custom Python scripts).
- Transformation logic was locked inside proprietary GUI tools or unversioned procedural SQL scripts (`CREATE OR REPLACE PROCEDURE ...`).

The emergence of **cloud-native, decoupled-storage data warehouses** (Amazon Redshift, Google BigQuery, Snowflake) in the 2010s inverted this paradigm:
- Storage became practically infinite and cheap.
- Massively Parallel Processing (MPP) compute clusters could scale on demand.
- Organizations shifted to **ELT (Extract, Load, Transform)**: landing raw data directly into the warehouse and executing transformations inside the engine using SQL.

```
   TRADITIONAL ETL (Compute on external server):
   [ Source ] ──► [ External ETL Server (Transform) ] ──► [ Data Warehouse (Target) ]

   MODERN ELT with dbt (Transform in-engine):
   [ Source ] ──► [ Data Warehouse (Raw Landing) ] ──► [ dbt Engine (Pushdown SQL Transform) ]
```

### Fishtown Analytics & Software Engineering for Data

In 2016, Tristan Handy, Drew Banin, and Connor McArthur founded **Fishtown Analytics** (later rebranded to **dbt Labs**). They observed a fundamental dysfunction in data teams:
- Software engineers had mature tooling: Git, automated unit tests, CI/CD, modular code reuse (DRY principle), and documentation.
- Data analysts and engineers were writing 2,000-line monolithic SQL scripts, copying and pasting code, running ad-hoc queries, and discovering broken pipelines only when executives reported wrong dashboard numbers.

dbt was created to introduce the **Analytics Engineering** discipline—giving data practitioners software engineering tools tailored for SQL.

---

## 3. The Modern Streamhouse / Lakehouse Ecosystem Context

### Positioning dbt in the Data-Lake-Stream-House Stack

In the modern enterprise architecture, dbt occupies the **Semantic Transformation & Governance Layer**:

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         ENTERPRISE LAKEHOUSE / STREAMHOUSE                               │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│  CONTINUOUS STREAMING INGESTION                                                          │
│  Kafka / NiFi / Flink CDC ──► [ Bronze Raw Tables (Iceberg / Paimon / Delta Lake) ]       │
│                                           │                                              │
│                                           ▼                                              │
│  ANALYTICS ENGINEERING & BATCH TRANSFORMATION                                            │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                     DBT                                            │  │
│  │  • Staging: Cleansing, casting, renaming raw stream/CDC tables                     │  │
│  │  • Intermediate: Joining dimensions, state reconstruction, business logic          │  │
│  │  • Marts: Star schemas, fact tables, dimensional aggregates, incremental rollups   │  │
│  │  • Semantic Layer: Metric definitions (Revenue, Churn, Active Users)               │  │
│  └────────────────────────────────────┬───────────────────────────────────────────────┘  │
│                                       │ Pushdown Query Execution                         │
│                                       ▼                                                  │
│  [ MPP ENGINES / LAKEHOUSE QUERY PLATFORMS ]                                             │
│  Trino / StarRocks / Snowflake / Databricks SQL / Google BigQuery / ClickHouse           │
│                                       │                                                  │
│                                       ▼                                                  │
│  [ BUSINESS SERVING LAYER ]                                                              │
│  BI Tools (Superset, PowerBI, Lightdash) / Reverse ETL / Predictive Analytics            │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### dbt with Trino, StarRocks, Snowflake, BigQuery, and Databricks
- **dbt + Trino:** dbt compiles SQL queries executed by Trino across distributed object storage (S3/ADLS) writing to Apache Iceberg or Hive tables.
- **dbt + StarRocks:** Accelerates real-time OLAP marts by maintaining StarRocks Primary Key tables and asynchronous Materialized Views.
- **dbt + Cloud Data Warehouses (Snowflake, BigQuery, Databricks):** Executes in-warehouse compute, managing micro-partitions, cluster keys, and warehouse sizing.

---

## 4. High-Level Architectural Topology

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                   DBT ARCHITECTURE                                       │
│                                                                                          │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                 DEVELOPER CODE                                     │  │
│  │  • SQL Models (`.sql`) with `{{ config() }}` and `{{ ref() }}`                     │  │
│  │  • Schema & Test Definitions (`.yml`)                                              │  │
│  │  • Jinja Macros (`.sql`) & Seed CSVs (`.csv`)                                      │  │
│  └─────────────────────────────────────┬──────────────────────────────────────────────┘  │
│                                        │ Parsed & Compiled                               │
│                                        ▼                                                 │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                         DBT CORE ENGINE (COMPILER & RUNNER)                        │  │
│  │  ┌────────────────────────┐  ┌─────────────────────────┐  ┌─────────────────────┐  │  │
│  │  │ Jinja Compiler & Parser│  │ DAG Topological Sorter  │  │ Multi-Thread Worker │  │  │
│  │  │ (Resolves macros/refs) │  │ (Builds dependency graph│  │  Task Pool          │  │  │
│  │  └────────────────────────┘  └─────────────────────────┘  └─────────────────────┘  │  │
│  │  ┌──────────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │ Target Adapter Plugin (`dbt-trino`, `dbt-snowflake`, `dbt-starrocks`, etc.)    │  │  │
│  │  └──────────────────────────────────────────────────────────────────────────────┘  │  │
│  └─────────────────────────────────────┬──────────────────────────────────────────────┘  │
│                                        │ Generates DDL/DML & Executes Queries via JDBC   │
│                                        ▼                                                 │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                            TARGET DATA WAREHOUSE / QUERY ENGINE                    │  │
│  │                     Trino / StarRocks / Snowflake / BigQuery / Databricks          │  │
│  └────────────────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. How dbt Processes Data at a High Level

### Code Abstractions: Models, Sources, Seeds, Snapshots

1. **Models:** SQL files containing a single `SELECT` statement. A model represents a table or view in the warehouse.
   ```sql
   -- models/marts/fct_orders.sql
   {{ config(materialized='incremental', unique_key='order_id') }}

   select
       order_id,
       customer_id,
       order_date,
       total_amount
   from {{ ref('stg_orders') }}
   {% if is_incremental() %}
       where order_date >= (select max(order_date) from {{ this }})
   {% endif %}
   ```
2. **Sources:** Declarative definitions of raw data tables loaded by external tools (e.g., Kafka connect landing tables). Enables freshness monitoring (`dbt source freshness`).
3. **Seeds:** Static CSV files version-controlled in Git (e.g., country codes, tax brackets) loaded into the warehouse via `dbt seed`.
4. **Snapshots:** Implement **Slowly Changing Dimensions (SCD Type 2)** automatically by recording historical row updates with `valid_from` and `valid_to` timestamps.

### Materialization Strategies

| Materialization | Generated SQL Behavior | Use Case |
| :--- | :--- | :--- |
| **`view`** | `CREATE OR REPLACE VIEW AS SELECT ...` | Fast development, lightweight transforms, zero storage cost. |
| **`table`** | `CREATE TABLE AS SELECT ...` (CTAS) or atomic swap | Heavy queries, marts accessed by BI tools, fast query read performance. |
| **`incremental`** | Inserts/merges only new or modified rows since the last dbt run | Massive datasets (billions of rows), event streams, CDC feeds. |
| **`ephemeral`** | Injected as a Common Table Expression (`WITH ... AS`) into downstream models | Code reuse without creating physical database objects. |

---

## 6. dbt vs. Alternative Transformation Approaches

| Dimension | dbt | Spark SQL / PySpark | Apache Airflow (standalone) | Stored Procedures |
| :--- | :--- | :--- | :--- | :--- |
| **Primary Focus** | SQL-first data modeling, testing, and documentation | Large-scale compute, unstructured data, ML | Task-level workflow orchestration | Legacy database procedural logic |
| **Compute Engine** | Pushdown (uses target warehouse/query engine) | Dedicated Spark cluster compute | External servers or pushdown scripts | RDBMS server engine |
| **Testing & CI/CD** | Native, out-of-the-box assertions & Slim CI | Requires custom pytest / Great Expectations | Requires custom Python testing tasks | Manual, error-prone |
| **Dependency Management** | Automatic DAG inference via `{{ ref() }}` | Manual script sequencing | Explicit Python DAG operators (`>>`) | Manual execution scripts |
| **Learning Curve** | Low (Standard SQL + basic Jinja) | Medium-High (Python, Scala, Spark internals) | Medium (Python DAG authoring) | Low-Medium (Vendor SQL dialects) |

---

## 7. Key Terminology & Mental Model Glossary

- **`ref()`:** The core Jinja function used to reference another dbt model. It builds the DAG dependency graph and resolves to the target schema table name.
- **`source()`:** Jinja function used to reference external raw tables declared in YAML.
- **`manifest.json`:** The single source of truth artifact compiled by dbt containing the full metadata, dependency graph, tests, and configuration of the project.
- **Incremental Strategy:** The algorithm used to update incremental models (`merge`, `insert_overwrite`, `delete+insert`, `append`, `microbatch`).
- **Jinja Macro:** Reusable, parameterized SQL snippets (functions) written in Jinja.
- **Generic Test:** Reusable tests defined in YAML (e.g., `unique`, `not_null`).
- **Singular Test:** Custom SQL queries in the `tests/` directory that fail if they return any rows.
- **dbt Mesh:** Multi-project architecture allowing distinct teams to govern their own dbt projects and share models across project boundaries using contracts and access controls.

---

## 8. References & Further Reading

1. **Official dbt Documentation:** [https://docs.getdbt.com/](https://docs.getdbt.com/)
2. **dbt Best Practices Guide:** [https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)
3. **The Analytics Engineering Roundup:** [https://roundup.getdbt.com/](https://roundup.getdbt.com/)
4. **dbt Semantic Layer:** [https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl](https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl)


---

**Layer:** 🔧 SQL Transformation & Modeling  
**Parent MOC:** [[MOCs/MOC_Transformation_and_OLAP_Serving|MOC: Transformation & OLAP Serving]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** → [[dbt/02_Data_Guide|Data Guide]]

**Related technologies:** [[Trino/01_Overview|Trino]] · [[StarRocks/01_Overview|StarRocks]] · [[Apache Hive/01_Overview|Apache Hive]] · [[Apache Iceberg/01_Overview|Apache Iceberg]]
