---
title: "dbt (data build tool): Architecture"
type: architecture
tags:
  - dbt
  - sql-transformation
  - data-modeling
  - analytics-engineering
  - dag
  - 03-architecture
aliases:
  - "dbt Architecture"
  - "dbt DAG"
  - "dbt Manifest"
layer: "SQL Transformation & Modeling"
parent: "[[MOCs/MOC_Transformation_and_OLAP_Serving]]"
---

# dbt (data build tool): Architecture & Execution Engine

## Table of Contents
1. [Core Engine Architecture](#1-core-engine-architecture)
2. [Compilation & Manifest Lifecycle](#2-compilation--manifest-lifecycle)
3. [DAG Dependency Resolution & Schedulers](#3-dag-dependency-resolution--schedulers)
4. [The Adapter Layer: Translating SQL to Target Dialects](#4-the-adapter-layer-translating-sql-to-target-dialects)
5. [Multi-Threaded Execution Engine](#5-multi-threaded-execution-engine)
6. [dbt Mesh & Multi-Project Architectures](#6-dbt-mesh--multi-project-architectures)
7. [The dbt Semantic Layer & Metrics](#7-the-dbt-semantic-layer--metrics)
8. [dbt Core vs. dbt Cloud](#8-dbt-core-vs-dbt-cloud)
9. [References & Further Reading](#9-references--further-reading)

---

## 1. Core Engine Architecture

dbt Core is a Python-based execution engine that does **not** process data in its own memory. Instead, it acts as a compiler, DAG scheduler, and query coordinator.

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    DBT CORE RUNTIME                                     │
│                                                                                         │
│  ┌──────────────────────┐    ┌───────────────────────────┐    ┌──────────────────────┐  │
│  │   Jinja2 Compiler    │───►│ Manifest Graph Generator  │───►│ Multi-Thread Worker  │  │
│  │ (Resolves ref/source)│    │ (Topological Sort / DAG)  │    │ Task Pool (N Threads)│  │
│  └──────────────────────┘    └───────────────────────────┘    └──────────┬───────────┘  │
│                                                                          │              │
│  ┌───────────────────────────────────────────────────────────────────────▼───────────┐  │
│  │                           DATABASE ADAPTER PLUGIN LAYER                            │  │
│  │  ┌──────────────────┐ ┌───────────────────┐ ┌─────────────────┐ ┌────────────────┐ │  │
│  │  │  dbt-snowflake   │ │   dbt-bigquery    │ │    dbt-trino    │ │ dbt-starrocks  │ │  │
│  │  └──────────────────┘ └───────────────────┘ └─────────────────┘ └────────────────┘ │  │
│  └───────────────────────────────────────┬────────────────────────────────────────────┘  │
└──────────────────────────────────────────┼──────────────────────────────────────────────┘
                                           │ Issues DDL / DML Queries over TCP / TLS
                                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           TARGET MPP / LAKEHOUSE STORAGE                                │
│                          Snowflake / BigQuery / Trino / StarRocks                       │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Compilation & Manifest Lifecycle

When you execute `dbt run` or `dbt compile`, dbt executes the following sequential phases:

```
[ Read Files ] ──► [ Jinja Parse ] ──► [ Build AST ] ──► [ Generate manifest.json ] ──► [ SQL Dispatch ]
```

1. **Discovery & Parsing:** Scans all `.sql`, `.yml`, `.csv`, and `.py` files in the project.
2. **Jinja Evaluation:** Evaluates Jinja blocks, resolves variables (`var('env')`), and executes macros.
3. **Graph Construction:** Replaces `{{ ref('stg_orders') }}` with fully-qualified database relations (e.g., `analytics.staging.stg_orders`) and adds directed edges to the graph.
4. **Manifest Artifact Generation:** Emits `target/manifest.json`, the comprehensive JSON representation of the entire project topology, schema contracts, tests, and configuration.

---

## 3. DAG Dependency Resolution & Schedulers

dbt builds a mathematical **Directed Acyclic Graph (DAG)** using topological sorting.

- **Independent Nodes Run in Parallel:** Nodes at the same topological depth execute concurrently across available worker threads.
- **Fail-Fast Cascading:** If an upstream model fails its execution or test, all downstream dependent models are skipped (`SKIP`), preventing corrupted data from populating reporting tables.

```
   [ stg_customers ] ──┐
                       ├──► [ int_customer_orders ] ──► [ dim_customers ] (Mart)
   [ stg_orders ]    ──┤
                       └──► [ fct_orders ] ──────────────────────────────► [ Monthly Revenue ] (Metric)
   [ stg_payments ]  ──┘
```

---

## 4. The Adapter Layer: Translating SQL to Target Dialects

dbt decouples user SQL from warehouse-specific DDL syntax using **Adapters**.

An Adapter implements database-specific behavior for:
- Creating views, tables, and temporary staging relations.
- Executing atomic swap operations (e.g., `ALTER TABLE swap_table RENAME TO target_table`).
- Formatting `MERGE` statements and upsert queries.
- Managing database connections and transactions.

```python
# Example conceptual adapter method in Python
class SnowflakeAdapter(BaseAdapter):
    def get_create_table_as_sql(self, relation, sql):
        return f"CREATE OR REPLACE TRANSIENT TABLE {relation} AS {sql};"
```

---

## 5. Multi-Threaded Execution Engine

In `profiles.yml`, the `threads` setting defines how many parallel database connections dbt opens simultaneously:

```yaml
# profiles.yml
my_dbt_project:
  target: prod
  outputs:
    prod:
      type: trino
      host: trino-coordinator.internal
      port: 443
      threads: 8   # Concurrently executes up to 8 independent models
      schema: analytics
```

- If `threads: 8`, dbt polls the dependency graph, grabs up to 8 unblocked nodes, compiles their SQL, and dispatches them to Trino/Snowflake in parallel.
- When a model finishes, dbt unblocks its downstream children and dispatches the next available task.

---

## 6. dbt Mesh & Multi-Project Architectures

For large enterprises with multiple data teams, a single monolithic dbt project creates slow compilation, merge conflicts, and unclear ownership.

**dbt Mesh** enables a decentralized **Data Mesh** pattern:
- **Producer Projects (Core Data Team):** Maintains foundational models (`dim_customers`, `dim_products`) marked as `public` with enforced model contracts.
- **Consumer Projects (Marketing/Finance Teams):** Cross-reference core models using `{{ ref('core_project', 'dim_customers') }}` without re-compiling the upstream codebase.

```
┌────────────────────────────────┐         ┌─────────────────────────────────┐
│      CORE DATA PLATFORM        │         │        MARKETING ANALYTICS      │
│  [ public: dim_customers ] ────┼─────────┼─► [ mart_campaign_conversions ] │
│  (Contract Enforced: SLA Safe) │ Cross-  │                                 │
└────────────────────────────────┘ Project └─────────────────────────────────┘
```

---

## 7. The dbt Semantic Layer & Metrics

The **dbt Semantic Layer** decouples business metric logic from visualization tools. Instead of writing `SUM(revenue)` in 10 different dashboards (risking inconsistent formulas), metrics are defined once in dbt:

```yaml
# models/marts/metrics.yml
semantic_models:
  - name: orders
    model: ref('fct_orders')
    entities:
      - name: order_id
        type: primary
    measures:
      - name: total_revenue
        agg: sum
        expr: amount_usd
      - name: order_count
        agg: count
        expr: order_id

metrics:
  - name: revenue_per_customer
    type: derived
    type_params:
      expr: total_revenue / count_distinct(customer_id)
```

BI tools (Tableau, PowerBI, Superset) query the Semantic Layer via JDBC/GraphQL, ensuring consistent metric definitions company-wide.

---

## 8. dbt Core vs. dbt Cloud

| Feature | dbt Core (Open Source) | dbt Cloud (Enterprise SaaS) |
| :--- | :--- | :--- |
| **Licensing** | Apache 2.0 (Free) | Commercial SaaS / Enterprise VPC |
| **Interface** | CLI (`dbt run`, `dbt test`) | Web IDE, Visual Job Scheduler, Metric Explorer |
| **Orchestration** | Requires external scheduler (Airflow, Dagster, Prefect) | Built-in managed scheduler with webhooks & alerts |
| **CI / CD** | Manual GitHub Actions / GitLab CI setup | Turnkey **Slim CI** (runs only modified models on PR) |
| **Semantic Layer** | Local MetricFlow CLI | Hosted Semantic Layer API with high-concurrency caching |
| **Security & RBAC** | Managed by local machine / IAM | Single Sign-On (SSO / SAML), audit logs, RBAC |

---

## 9. References & Further Reading

1. **dbt Architecture Internals:** [https://docs.getdbt.com/docs/core-concepts/dbt-architecture](https://docs.getdbt.com/docs/core-concepts/dbt-architecture)
2. **dbt Adapters Documentation:** [https://docs.getdbt.com/docs/supported-data-platforms](https://docs.getdbt.com/docs/supported-data-platforms)
3. **dbt Mesh Multi-Project Setup:** [https://docs.getdbt.com/docs/collaborate/govern/about-mesh](https://docs.getdbt.com/docs/collaborate/govern/about-mesh)
4. **dbt Semantic Layer Architecture:** [https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl](https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl)


---

**Layer:** 🔧 SQL Transformation & Modeling  
**Parent MOC:** [[MOCs/MOC_Transformation_and_OLAP_Serving|MOC: Transformation & OLAP Serving]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** ← [[dbt/02_Data_Guide|Data Guide]]  |  → [[dbt/04_Performance_Guide|Performance Guide]]

**Related technologies:** [[Trino/01_Overview|Trino]] · [[StarRocks/01_Overview|StarRocks]] · [[Apache Hive/01_Overview|Apache Hive]] · [[Apache Iceberg/01_Overview|Apache Iceberg]]
