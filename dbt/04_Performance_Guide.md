# dbt (data build tool): Performance Optimization & Enterprise Best Practices

## Table of Contents
1. [Warehouse Cost & Query Optimization](#1-warehouse-cost--query-optimization)
2. [Incremental Model Tuning & Microbatching](#2-incremental-model-tuning--microbatching)
3. [Clustering, Partitioning & Query Pruning](#3-clustering-partitioning--query-pruning)
4. [Multi-Threading & Concurrency Tuning](#4-multi-threading--concurrency-tuning)
5. [Slim CI/CD Execution (`state:modified+`)](#5-slim-cicd-execution-statemodified)
6. [Testing Optimization & Guardrails](#6-testing-optimization--guardrails)
7. [Common Anti-Patterns & Pitfalls](#7-common-anti-patterns--pitfalls)
8. [Best Practices for Junior Data Engineers](#8-best-practices-for-junior-data-engineers)
9. [Performance Monitoring & Telemetry](#9-performance-monitoring--telemetry)
10. [References & Further Reading](#10-references--further-reading)

---

## 1. Warehouse Cost & Query Optimization

Because dbt executes transformations directly inside your target database, **dbt performance equals warehouse query performance**.

### 1.1 Model Selection Strategies (`--select`)

Never run full project builds (`dbt run`) repeatedly during local development or hourly schedules. Use selector syntax:

```bash
# Run a specific model and all its downstream children
dbt run --select fct_orders+

# Run a specific model and its immediate 2 upstream parents
dbt run --select 2+fct_orders

# Run only models with a specific tag (e.g., hourly_marts)
dbt run --select tag:hourly_marts

# Run models in the marts folder excluding snapshots
dbt run --select path:models/marts --exclude path:models/snapshots
```

### 1.2 Materialization Choice Matrix

```
       [ Query Complexity / Data Volume ]
                     │
       ┌─────────────┴─────────────┐
       ▼                           ▼
[ Low Volume / Simple ]    [ High Volume / Complex ]
       │                           │
       ▼                           ▼
  Use `view`                  Use `table`
 (Zero storage cost)     (Precomputed for fast BI)
                                   │
                                   ▼
                       [ Massive Data > 10M rows ]
                                   │
                                   ▼
                         Use `incremental`
                       (Process only new delta)
```

---

## 2. Incremental Model Tuning & Microbatching

### 2.1 The Incremental Filter Guardrail

The most common performance bug in dbt is forgetting to filter source tables inside `is_incremental()`:

```sql
-- BAD: Scans 100% of raw table on every run before performing MERGE
select * from {{ ref('stg_events') }}
{% if is_incremental() %}
    -- MISSING WHERE FILTER!
{% endif %}

-- GOOD: Scans only the latest 3-day sliding window of source data
select * from {{ ref('stg_events') }}
{% if is_incremental() %}
    where event_timestamp >= (select max(event_timestamp) from {{ this }}) - interval '3 day'
{% endif %}
```

### 2.2 Sizing the Lookback Window
- **Why use a lookback window (e.g., `3 day`)?** Distributed upstream systems (Kafka, mobile apps) produce late-arriving events. A lookback window captures late events while keeping the scan volume small.
- **Handling Schema Drift:** Set `on_schema_change='append_new_columns'` or `fail` in the model config.

### 2.3 Microbatching (dbt 1.9+)

For massive historical backfills, running a single multi-terabyte incremental run risks query timeout and warehouse OOM.

**Microbatching** instructs dbt to automatically split historical ranges into smaller, deterministic daily/hourly intervals:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    unique_key='event_id',
    event_time='event_timestamp',
    batch_size='day',
    begin='2024-01-01'
) }}

select * from {{ ref('stg_events') }}
```

When triggered with `--event-time-start "2025-01-01" --event-time-end "2025-01-07"`, dbt executes 7 small independent daily batch queries in sequence.

---

## 3. Clustering, Partitioning & Query Pruning

Pushdown performance relies on aligning dbt DDL configurations with the physical layout of the target database:

### Snowflake (Clustering Keys)
```sql
{{ config(
    materialized='incremental',
    cluster_by=['date_trunc(month, order_date)', 'customer_id']
) }}
```

### BigQuery (Partitioning & Clustering)
```sql
{{ config(
    materialized='incremental',
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["country_code", "status"]
) }}
```

### Trino / Iceberg (Partition Spec)
```sql
{{ config(
    materialized='table',
    properties={
      "partitioning": "ARRAY['day(order_date)']"
    }
) }}
```

---

## 4. Multi-Threading & Concurrency Tuning

In `profiles.yml`, setting `threads` controls execution throughput:

```
           [ dbt Schedulers: threads: 4 ]
                         │
     ┌───────────┬───────┴───────────┬───────────┐
     ▼           ▼                   ▼           ▼
[ Model A ] [ Model B ]         [ Model C ] [ Model D ]
  (Worker 1)  (Worker 2)          (Worker 3)  (Worker 4)
     │           │                   │           │
     └───────────┴─────────┬─────────┴───────────┘
                           ▼
          [ Target Warehouse / Query Engine ]
```

### Thread Sizing Rules of Thumb:
- **Cloud Warehouses (Snowflake, BigQuery):** Set `threads: 8` to `16`. Cloud warehouses scale concurrency elastically.
- **Trino / StarRocks Clusters:** Set `threads: 4` to `8`. Avoid exceeding query queue capacity (`query.max-concurrent-queries`).
- **Single Postgres Instance:** Set `threads: 2` to `4` to prevent saturating the database CPU and connection pool.

---

## 5. Slim CI/CD Execution (`state:modified+`)

Running a full `dbt build` on every GitHub Pull Request costs thousands of dollars in warehouse compute and slows down deployment cycles.

### Slim CI Workflow:
By comparing the current PR code against the production `manifest.json` artifact, dbt identifies **only the models modified in the PR plus their direct downstream dependents**:

```bash
# 1. Download prod manifest.json into ./state-dir
# 2. Run and test ONLY changed models and their downstream children
dbt build --select state:modified+ --state ./state-dir --defer
```

- **`--defer`:** If a downstream model references an unchanged upstream table, dbt redirects the reference to the production table instead of failing because the upstream table wasn't built in the CI schema.
- **Result:** CI build times drop from 45 minutes to 90 seconds.

---

## 6. Testing Optimization & Guardrails

### 6.1 Avoid Redundant Tests
- Do not put `not_null` and `unique` on every single intermediate CTE. Place tests on **Staging (source boundaries)** and **Marts (business consumption boundaries)**.

### 6.2 Test Selection
```bash
# Run tests only for modified models
dbt test --select state:modified

# Run only critical unique tests during fast CI
dbt test --select test_type:generic,test_name:unique
```

### 6.3 Store Test Failures for Fast Debugging
```yaml
# dbt_project.yml
tests:
  +store_failures: true  # Saves failed test rows into a dedicated database schema for SQL inspection
```

---

## 7. Common Anti-Patterns & Pitfalls

| Anti-Pattern | Why It Fails | Correct Solution |
| :--- | :--- | :--- |
| **Monolithic 1,500-line Models** | Unreadable, impossible to test modularly, forces full rebuilds. | Break into Staging $\to$ Intermediate $\to$ Marts layer. |
| **Hardcoded Table Names (`from prod.orders`)** | Breaks environment isolation (dev vs. prod), breaks DAG lineage. | Always use `{{ ref('model_name') }}` or `{{ source(...) }}`. |
| **Missing `is_incremental()` filters** | Scans entire multi-billion row table every run, inflating bills. | Filter source query using `select max(...) from {{ this }}`. |
| **Overusing `ephemeral` materializations** | Inlines complex CTEs repeatedly into multiple downstream models, duplicating execution cost. | Switch to `table` or `incremental` for heavy transformations. |
| **Testing every intermediate column** | Runs hundreds of unnecessary queries on temporary views. | Test Staging source keys and final Mart deliverables. |
| **Running `dbt run` without tests in CI** | Allows bad code/data to reach production silently. | Always use `dbt build` (runs model $\to$ tests model immediately). |

---

## 8. Best Practices for Junior Data Engineers

### 8.1 The Standard 3-Layer Project Structure
```
models/
├── staging/          # 1:1 with raw sources; clean column names, cast types, filter deletes
│   ├── stripe/
│   │   ├── src_stripe.yml
│   │   ├── stg_stripe__payments.sql
│   │   └── stg_stripe__customers.sql
├── intermediate/     # Complex business logic, cross-source joins, state tracking
│   ├── int_customer_orders_joined.sql
│   └── int_orders_pivoted.sql
└── marts/            # Star schema facts and dimensions consumed by BI / analytics
    ├── core/
    │   ├── dim_customers.sql
    │   └── fct_orders.sql
    └── finance/
        └── fct_monthly_financial_metrics.sql
```

### 8.2 Production Deployment Checklist
- [ ] Every staging model uses `source()` and has a primary key with `unique` and `not_null` tests.
- [ ] All model-to-model dependencies use `ref()`.
- [ ] Large fact tables (>10M rows) configured as `incremental` with appropriate lookback window.
- [ ] Physical clustering/partitioning defined in model config for target database.
- [ ] Slim CI configured in GitHub Actions using `state:modified+` and `--defer`.
- [ ] `dbt source freshness` scheduled upstream before transformation runs.
- [ ] Documentation descriptions added for all critical Mart columns.

---

## 9. Performance Monitoring & Telemetry

### 9.1 `run_results.json` Analysis
After every execution, dbt writes `target/run_results.json` containing:
- Execution time per model in milliseconds.
- Status (`success`, `error`, `skipped`).
- Rows affected (adapter-dependent).

### 9.2 Integrating dbt-artifacts
Use the open-source package **`dbt-artifacts`** to automatically load `manifest.json` and `run_results.json` directly into your warehouse to build internal dashboards tracking model run durations, cost trends, and failure hotspots.

---

## 10. References & Further Reading

1. **dbt Performance Optimization Guide:** [https://docs.getdbt.com/guides/orchestration/custom-schemas-in-dbt/optimizing-runtime](https://docs.getdbt.com/guides/orchestration/custom-schemas-in-dbt/optimizing-runtime)
2. **Slim CI Implementation:** [https://docs.getdbt.com/docs/deploy/continuous-integration](https://docs.getdbt.com/docs/deploy/continuous-integration)
3. **dbt Microbatch Incremental Strategy:** [https://docs.getdbt.com/docs/build/microbatch](https://docs.getdbt.com/docs/build/microbatch)
4. **dbt Project Structure Best Practices:** [https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)
