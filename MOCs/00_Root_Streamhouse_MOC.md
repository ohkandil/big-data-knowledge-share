---
title: "Streamhouse & Lakehouse Knowledge Map"
type: moc
tags:
  - moc
  - streamhouse
  - lakehouse
  - data-engineering
  - architecture
aliases:
  - Root MOC
  - Streamhouse MOC
  - Knowledge Hub
---

# 🌐 Streamhouse & Lakehouse Architecture: Master Map of Content (MOC)

> Central nexus node for the distributed Big Data Knowledge Graph. This MOC maps data flow, technology layers, core distributed concepts, and cross-engine comparisons across the modern streamhouse stack.

---

## 🗺️ Architectural Layer MOCs (Sub-Hubs)

Explore the ecosystem through dedicated functional domain maps:

- 🚀 **[[MOCs/MOC_Stream_Processing_and_Logistics|MOC: Stream Processing & Data Logistics]]**
  - Continuous data movement, event ingestion, and stateful real-time stream computation.
  - *Tech nodes:* [[Apache Flink/01_Overview|Apache Flink]] · [[Apache Kafka/01_Overview|Apache Kafka]] · [[Apache NiFi/01_Overview|Apache NiFi]]
- 🧊 **[[MOCs/MOC_Lakehouse_Storage_and_Table_Formats|MOC: Lakehouse Storage & Table Formats]]**
  - Open data lakehouse storage, snapshot trees, LSM formats, and catalog registries.
  - *Tech nodes:* [[Apache Iceberg/01_Overview|Apache Iceberg]] · [[Apache Paimon/01_Overview|Apache Paimon]] · [[Apache Hive/01_Overview|Apache Hive]]
- 📊 **[[MOCs/MOC_Transformation_and_OLAP_Serving|MOC: Transformation & OLAP Serving]]**
  - Declarative SQL modeling, interactive cross-source federation, and sub-second real-time OLAP.
  - *Tech nodes:* [[dbt/01_Overview|dbt]] · [[Trino/01_Overview|Trino]] · [[StarRocks/01_Overview|StarRocks]]
- ⚖️ **[[MOCs/MOC_Architectural_Comparisons|MOC: Cross-Engine Architectural Comparisons]]**
  - High-impact decision matrices and side-by-side engineering trade-offs.
  - *Comparisons:* [[Comparisons/Trino_vs_StarRocks|Trino vs. StarRocks]] · [[Comparisons/Table_Formats_Iceberg_vs_Paimon_vs_Hive|Iceberg vs. Paimon vs. Hive]]

---

## 🧭 End-to-End Streamhouse Data Flow

```
[ Sources: APIs, DBs, IoT ]
            │
            ▼
   [[Apache NiFi/01_Overview|Apache NiFi]] (Data Logistics & Protocol Mediation)
            │
            ▼
   [[Apache Kafka/01_Overview|Apache Kafka]] (Distributed Immutable Commit Log)
            │
            ▼
   [[Apache Flink/01_Overview|Apache Flink]] (Stateful Stream Engine & CEP)
            │
            ▼ Sub-minute 2PC Commits
   ┌────────────────────────────────────────────────────────────────────────┐
   │                       OPEN LAKEHOUSE STORAGE TIER                      │
   │  • [[Apache Iceberg/01_Overview|Apache Iceberg]]: Canonical Snapshot Tree (OCC)      │
   │  • [[Apache Paimon/01_Overview|Apache Paimon]]: Streaming LSM Upserts on S3       │
   │  • [[Apache Hive/01_Overview|Apache Hive]]: Legacy Directory Metadata & HMS         │
   └───────────────────────────────────┬────────────────────────────────────┘
                                       │
         ┌─────────────────────────────┴─────────────────────────────┐
         ▼                                                           ▼
[[dbt/01_Overview|dbt]] (Batch SQL ELT Modeling)         [[Trino/01_Overview|Trino]] (Interactive Data Lake Federation)
                                                 [[StarRocks/01_Overview|StarRocks]] (Sub-Second Vectorized OLAP)
                                                                     │
                                                                     ▼
                                                  [ Business Dashboards, Reverse ETL, ML ]
```

---

## 🧠 Fundamental Distributed Computing Concepts

These atomic concept nodes form the structural lattice connecting technologies across the graph:

- ⏱️ **[[Concepts/Event_Time_and_Watermarking|Event Time & Watermarking]]** — Handling out-of-order data and deterministic time progression in [[Apache Flink/02_Data_Guide|Flink]] and [[Apache Kafka/02_Data_Guide|Kafka]].
- 🌲 **[[Concepts/LSM_Trees_and_Compaction|LSM-Trees & Compaction]]** — Log-Structured Merge trees powering [[Apache Paimon/02_Data_Guide|Paimon]], [[StarRocks/02_Data_Guide|StarRocks]], and Flink's RocksDB state backend.
- 🔒 **[[Concepts/Exactly_Once_Semantics_and_2PC|Exactly-Once Semantics & Two-Phase Commit]]** — End-to-end correctness guarantees between [[Apache Flink/03_Architecture|Flink]], [[Apache Kafka/03_Architecture|Kafka]], and [[Apache Iceberg/03_Architecture|Iceberg]].
- 🔄 **[[Concepts/Change_Data_Capture_CDC|Change Data Capture (CDC)]]** — Log-based real-time database replication into [[Apache Kafka/01_Overview|Kafka]], [[Apache Flink/01_Overview|Flink]], [[Apache Paimon/01_Overview|Paimon]], and [[StarRocks/01_Overview|StarRocks]].
- ⚡ **[[Concepts/Vectorized_SIMD_Execution|Vectorized SIMD Execution]]** — CPU cache locality and parallel instruction sets in [[StarRocks/03_Architecture|StarRocks]] and [[Trino/02_Data_Guide|Trino]].
- 🎯 **[[Concepts/Cost_Based_Optimizer_CBO|Cost-Based Optimizer (CBO)]]** — Relational algebra join reordering in [[Trino/03_Architecture|Trino]], [[StarRocks/03_Architecture|StarRocks]], and [[Apache Hive/03_Architecture|Hive]].
- 🛡️ **[[Concepts/Optimistic_Concurrency_Control_OCC|Optimistic Concurrency Control (OCC)]]** — Multi-engine ACID transactions in [[Apache Iceberg/03_Architecture|Iceberg]].
- 🚀 **[[Concepts/Zero_Copy_IO|Zero-Copy I/O & Memory Transfers]]** — Direct memory transfers and Linux `sendfile()` in [[Apache Kafka/03_Architecture|Kafka]] and [[Apache NiFi/03_Architecture|NiFi]].
- 📊 **[[Concepts/Columnar_Storage_and_MinMax_Stats|Columnar Storage & Min/Max Stats]]** — Parquet/ORC pruning in [[Apache Iceberg/02_Data_Guide|Iceberg]], [[Trino/02_Data_Guide|Trino]], and [[Apache Hive/02_Data_Guide|Hive]].
- ☁️ **[[Concepts/Decoupled_Compute_and_Storage|Decoupled Compute & Storage]]** — Stateless query processing over cloud object stores.

---

## 📂 Quick Navigator by Technology

| Technology | Overview | Data Guide | Architecture | Performance Guide |
| :--- | :--- | :--- | :--- | :--- |
| **Apache Flink** | [[Apache Flink/01_Overview\|Flink Overview]] | [[Apache Flink/02_Data_Guide\|Data Guide]] | [[Apache Flink/03_Architecture\|Architecture]] | [[Apache Flink/04_Performance_Guide\|Performance Guide]] |
| **Apache Kafka** | [[Apache Kafka/01_Overview\|Kafka Overview]] | [[Apache Kafka/02_Data_Guide\|Data Guide]] | [[Apache Kafka/03_Architecture\|Architecture]] | [[Apache Kafka/04_Performance_Guide\|Performance Guide]] |
| **Apache NiFi** | [[Apache NiFi/01_Overview\|NiFi Overview]] | [[Apache NiFi/02_Data_Guide\|Data Guide]] | [[Apache NiFi/03_Architecture\|Architecture]] | [[Apache NiFi/04_Performance_Guide\|Performance Guide]] |
| **dbt** | [[dbt/01_Overview\|dbt Overview]] | [[dbt/02_Data_Guide\|Data Guide]] | [[dbt/03_Architecture\|Architecture]] | [[dbt/04_Performance_Guide\|Performance Guide]] |
| **Trino** | [[Trino/01_Overview\|Trino Overview]] | [[Trino/02_Data_Guide\|Data Guide]] | [[Trino/03_Architecture\|Architecture]] | [[Trino/04_Performance_Guide\|Performance Guide]] |
| **StarRocks** | [[StarRocks/01_Overview\|StarRocks Overview]] | [[StarRocks/02_Data_Guide\|Data Guide]] | [[StarRocks/03_Architecture\|Architecture]] | [[StarRocks/04_Performance_Guide\|Performance Guide]] |
| **Apache Paimon** | [[Apache Paimon/01_Overview\|Paimon Overview]] | [[Apache Paimon/02_Data_Guide\|Data Guide]] | [[Apache Paimon/03_Architecture\|Architecture]] | [[Apache Paimon/04_Performance_Guide\|Performance Guide]] |
| **Apache Iceberg** | [[Apache Iceberg/01_Overview\|Iceberg Overview]] | [[Apache Iceberg/02_Data_Guide\|Data Guide]] | [[Apache Iceberg/03_Architecture\|Architecture]] | [[Apache Iceberg/04_Performance_Guide\|Performance Guide]] |
| **Apache Hive** | [[Apache Hive/01_Overview\|Hive Overview]] | [[Apache Hive/02_Data_Guide\|Data Guide]] | [[Apache Hive/03_Architecture\|Architecture]] | [[Apache Hive/04_Performance_Guide\|Performance Guide]] |
