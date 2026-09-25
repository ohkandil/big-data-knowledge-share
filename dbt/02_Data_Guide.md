---
title: "dbt (data build tool): Data Guide"
type: guide
tags:
  - dbt
  - sql-transformation
  - data-modeling
  - analytics-engineering
  - dag
  - 02-data-guide
aliases:
  - "dbt Data Guide"
  - "dbt Models"
  - "dbt Sources"
layer: "SQL Transformation & Modeling"
parent: "[[MOCs/MOC_Transformation_and_OLAP_Serving]]"
---

# dbt (data build tool): Data Processing & Modeling Guide

## Table of Contents
1. [Core Data Modeling Abstractions](#1-core-data-modeling-abstractions)
   - [Models (SQL & Python)](#models-sql--python)
   - [Sources & Freshness Verification](#sources--freshness-verification)
   - [Seeds for Static Reference Data](#seeds-for-static-reference-data)
   - [Snapshots (SCD Type 2 Historization)](#snapshots-scd-type-2-historization)
2. [Materialization Strategies Deep Dive](#2-materialization-strategies-deep-dive)
   - [View vs. Table Materialization](#view-vs-table-materialization)
   - [Ephemeral Models & CTE Inlining](#ephemeral-models--cte-inlining)
   - [Incremental Materializations & Strategies](#incremental-materializations--strategies)
3. [Jinja Templating, Macros, and Packages](#3-jinja-templating-macros-and-packages)
4. [Data Quality Testing & Assertions](#4-data-quality-testing--assertions)
5. [Model Governance & Model Contracts](#5-model-governance--model-contracts)
6. [Data Lineage & Documentation Generation](#6-data-lineage--documentation-generation)
7. [References & Further Reading](#7-references--further-reading)

---

## 1. Core Data Modeling Abstractions

### Models (SQL & Python)

A **dbt Model** is a single file in the `models/` directory representing a transformation step that compiles into a table or view in the warehouse.

- **SQL Models (`.sql`):** Pure `SELECT` statements parameterized with Jinja.
- **Python Models (`.py`):** Supported on Snowflake, Databricks, and BigQuery. Executes data transformations using PySpark or Snowpark DataFrames for data science/complex transformations.

```sql
-- models/staging/stg_stripe_payments.sql
with source as (
    select * from {{ source('stripe', 'payment') }}
),

renamed as (
    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status,
        -- Convert cents to dollars
        amount / 100.0 as amount_usd,
        created as created_at
    from source
)

select * from renamed
```

### Sources & Freshness Verification

Sources allow you to name and describe raw tables loaded by external ingestion pipelines (e.g., Fivetran, Kafka Connect, NiFi).

```yaml
# models/staging/src_stripe.yml
version: 2

sources:
  - name: stripe
    database: raw_lake
    schema: stripe
    loaded_at_field: _loaded_at
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    tables:
      - name: payment
        description: Raw payment authorization events from Stripe API.
```

Running `dbt source freshness` checks table load timestamps, alerting data engineers if upstream ingestion pipelines stall.

### Seeds for Static Reference Data

**Seeds** are CSV files placed in the `seeds/` directory. Running `dbt seed` uploads the CSVs directly into the data warehouse as physical tables.

- *Ideal for:* Country code lookups, ISO currency mappings, marketing campaign categorization tables.
- *Anti-Pattern:* Loading transactional data (>100MB) via seeds.

### Snapshots (SCD Type 2 Historization)

Snapshots automatically track historical changes in mutable source tables over time using **Slowly Changing Dimensions (SCD Type 2)**:

```sql
-- snapshots/snap_customers.sql
{% snapshot snap_customers %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

select * from {{ source('crm', 'customers') }}

{% endsnapshot %}
```

dbt automatically manages `dbt_valid_from`, `dbt_valid_to`, and `dbt_scd_id` columns, recording a new row version whenever `updated_at` changes.

---

## 2. Materialization Strategies Deep Dive

### View vs. Table Materialization

- **`view`:** Translates to `CREATE OR REPLACE VIEW ... AS SELECT`.
  - Zero storage footprint. Always reflects latest source data.
  - Slower query time if underlying query is complex or joins multiple large tables.
- **`table`:** Translates to `CREATE TABLE ... AS SELECT` (CTAS) or writes to a temporary staging table followed by an atomic rename swap.
  - Faster query execution for BI users.
  - Higher storage and compute cost during dbt runs.

### Ephemeral Models & CTE Inlining

An `ephemeral` model does not create any physical object in the database. Instead, dbt interpolates its SQL as a **Common Table Expression (CTE)** into all downstream models that reference it.

```sql
-- models/intermediate/int_active_users.sql (materialized='ephemeral')
select user_id, last_active_date
from {{ ref('stg_users') }}
where status = 'active'
```

Downstream models referencing `int_active_users` receive:
```sql
with __dbt__cte__int_active_users as (
    select user_id, last_active_date from stg_users where status = 'active'
)
select ... from __dbt__cte__int_active_users ...
```

### Incremental Materializations & Strategies

Incremental models only process new or modified data since the previous run, cutting warehouse costs by up to 90%.

```sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

select
    transaction_id,
    user_id,
    amount,
    transaction_timestamp
from {{ ref('stg_transactions') }}

{% if is_incremental() %}
    -- Filter rows newer than the latest timestamp in the target table
    where transaction_timestamp >= (select max(transaction_timestamp) from {{ this }}) - interval '3 day'
{% endif %}
```

#### Incremental Strategies Matrix:

| Strategy | Mechanism | Best For | Engine Support |
| :--- | :--- | :--- | :--- |
| **`merge`** | Issues SQL `MERGE INTO target USING source ON target.id = source.id` | Upserts with mutable records | Snowflake, BigQuery, Databricks, StarRocks |
| **`delete+insert`** | Deletes matching keys from target, then inserts new rows | Warehouses without native `MERGE` | Redshift, Postgres, Trino (Hive/Iceberg) |
| **`insert_overwrite`** | Replaces whole partitions dynamically | Partitioned append-only event streams | BigQuery, Spark/Databricks, Snowflake |
| **`append`** | Blind `INSERT INTO target` | Pure append-only audit logs / clickstreams | All adapters |
| **`microbatch`** (dbt 1.9+) | Slices ingestion into deterministic hourly/daily time windows | Large-scale event processing with automatic backfilling | Snowflake, BigQuery, Databricks |

---

## 3. Jinja Templating, Macros, and Packages

### Jinja Templating Engine

dbt embeds the Jinja templating language inside SQL, allowing conditional logic, loops, and environment branching:

```sql
-- Pivot payments by payment method dynamically
{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

select
    order_id,
    {% for method in payment_methods %}
        sum(case when payment_method = '{{ method }}' then amount else 0 end) as {{ method }}_amount
        {%- if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('stg_payments') }}
group by 1
```

### Macros & Packages

Macros are reusable SQL functions defined in the `macros/` directory.

- **`dbt-utils`:** Essential package providing cross-database SQL helpers (`generate_surrogate_key`, `pivot`, `dateadd`, `star`).
- **`dbt-expectations`:** Great Expectations-style assertions inside dbt.

---

## 4. Data Quality Testing & Assertions

dbt supports two types of tests:

### Generic Tests (YAML-defined)
Defined directly against model columns in `.yml` files:
```yaml
models:
  - name: fct_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: status
        tests:
          - accepted_values:
              values: ['placed', 'shipped', 'completed', 'returned']
      - name: customer_id
        tests:
          - relationships:
              to: ref('dim_customers')
              field: customer_id
```

### Singular Tests (Custom SQL)
A `.sql` file in `tests/`. If the query returns **zero rows**, the test passes. If any row is returned, the test fails.
```sql
-- tests/assert_total_payment_is_positive.sql
select
    order_id,
    sum(total_amount) as total_amount
from {{ ref('fct_orders') }}
group by 1
having sum(total_amount) < 0
```

---

## 5. Model Governance & Model Contracts

In enterprise teams, breaking schema changes can cascade downstream. dbt introduced **Model Contracts** to enforce strict interface definitions:

```yaml
models:
  - name: dim_customers
    config:
      contract:
        enforced: true
    columns:
      - name: customer_id
        data_type: string
        constraints:
          - type: not_null
          - type: primary_key
      - name: signup_date
        data_type: date
```

If a developer alters the SQL query to return an `integer` instead of `string`, dbt rejects compilation before touching the warehouse.

---

## 6. Data Lineage & Documentation Generation

Running `dbt docs generate` produces static web assets (`index.html`, `manifest.json`, `catalog.json`):
- **Interactive DAG Visualizer:** Shows end-to-end lineage from source to mart.
- **Column Dictionaries:** Markdown descriptions, data types, and test coverage.
- **Compiled SQL Viewer:** Compare raw Jinja SQL with the compiled native SQL executed on the database.

---

## 7. References & Further Reading

1. **dbt Materializations:** [https://docs.getdbt.com/docs/build/materializations](https://docs.getdbt.com/docs/build/materializations)
2. **Incremental Models Guide:** [https://docs.getdbt.com/docs/build/incremental-models](https://docs.getdbt.com/docs/build/incremental-models)
3. **dbt Model Contracts & Governance:** [https://docs.getdbt.com/docs/collaborate/govern/model-contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts)
4. **dbt-utils Package:** [https://hub.getdbt.com/dbt-labs/dbt_utils/latest/](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/)


---

**Layer:** 🔧 SQL Transformation & Modeling  
**Parent MOC:** [[MOCs/MOC_Transformation_and_OLAP_Serving|MOC: Transformation & OLAP Serving]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** ← [[dbt/01_Overview|Overview & Foundational Concepts]]  |  → [[dbt/03_Architecture|Architecture]]

**Related technologies:** [[Trino/01_Overview|Trino]] · [[StarRocks/01_Overview|StarRocks]] · [[Apache Hive/01_Overview|Apache Hive]] · [[Apache Iceberg/01_Overview|Apache Iceberg]]
