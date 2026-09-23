<div align="center">

# 🏗️ Big Data Knowledge Share

### Production-Grade Deep-Dive Engineering Guides for the Modern Lakehouse & Streamhouse Stack

**Apache Flink · Apache Kafka · Apache NiFi · dbt · Trino · StarRocks · Apache Paimon · Apache Iceberg · Apache Hive**

[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-e6526f?style=for-the-badge&logo=apache%20flink&logoColor=white)](https://flink.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231f20?style=for-the-badge&logo=apache%20kafka&logoColor=white)](https://kafka.apache.org/)
[![Apache NiFi](https://img.shields.io/badge/Apache%20NiFi-618c39?style=for-the-badge&logo=apache%20nifi&logoColor=white)](https://nifi.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Trino](https://img.shields.io/badge/Trino-DD00A1?style=for-the-badge&logo=trino&logoColor=white)](https://trino.io/)
[![StarRocks](https://img.shields.io/badge/StarRocks-0052FF?style=for-the-badge&logo=starrocks&logoColor=white)](https://www.starrocks.io/)
[![Apache Paimon](https://img.shields.io/badge/Apache%20Paimon-5394EC?style=for-the-badge&logo=apache&logoColor=white)](https://paimon.apache.org/)
[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-0083B0?style=for-the-badge&logo=apache&logoColor=white)](https://iceberg.apache.org/)
[![Apache Hive](https://img.shields.io/badge/Apache%20Hive-FDEE21?style=for-the-badge&logo=apachehive&logoColor=black)](https://hive.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)](LICENSE)

*Comprehensive companion guides written for Data Engineers working on high-throughput, low-latency, distributed architectures.*

</div>

---

## 🧭 Modern Lakehouse / Streamhouse Topology

Every technology in this repository fulfills a dedicated architectural role within the modern enterprise data platform:

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                         MODERN STREAMHOUSE & LAKEHOUSE ARCHITECTURE                      │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│  1. INGESTION & DATA LOGISTICS (Edge & Ingest Tier)                                      │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ 🟢 APACHE NIFI: Multi-protocol logistics (SFTP, REST, DBs) ──► Validation & Routing│  │
│  └───────────────────────────────────┬────────────────────────────────────────────────┘  │
│                                      │                                                   │
│                                      ▼                                                   │
│  2. DISTRIBUTED EVENT LOG (Central Messaging Backbone)                                  │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ ⚫ APACHE KAFKA: Immutable, partitioned, replicated commit log (KRaft consensus)   │  │
│  └───────────────────────────────────┬────────────────────────────────────────────────┘  │
│                                      │                                                   │
│                                      ▼                                                   │
│  3. CONTINUOUS STREAM PROCESSING (Compute & Streaming ETL)                              │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ 🔴 APACHE FLINK: Stateful event-time processing, windowing, sub-10ms CEP & joins   │  │
│  └───────────────────────────────────┬────────────────────────────────────────────────┘  │
│                                      │ 2PC Checkpoint Commits (Sub-minute)               │
│                                      ▼                                                   │
│  4. OPEN TABLE STORAGE FORMATS (Object Storage: S3 / ADLS / GCS)                         │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ 🧊 APACHE ICEBERG: Canonical snapshot tree standard (Multi-engine ACID, OCC)        │  │
│  │ 🌊 APACHE PAIMON:  LSM-tree on S3 for streaming CDC upserts & changelog generation  │  │
│  │ 🐝 APACHE HIVE:    Legacy directory-mapped baseline catalog & Metastore (HMS)      │  │
│  └───────────────────────────────────┬────────────────────────────────────────────────┘  │
│                                      │                                                   │
│         ┌────────────────────────────┴────────────────────────────┐                      │
│         ▼                                                         ▼                      │
│  5. BATCH TRANSFORMATION & MODELING      6. INTERACTIVE OLAP & FEDERATED SERVING         │
│  ┌──────────────────────────────────┐    ┌──────────────────────────────────────────┐    │
│  │ 🟠 dbt (data build tool)         │    │ 🟣 TRINO: Interactive data lake          │    │
│  │ • Pushdown SQL modeling (ELT)    │    │   federation across 30+ disparate sources│    │
│  │ • Incremental & Microbatch DAGs  │    │ 🔵 STARROCKS: Sub-second C++ SIMD        │    │
│  │ • Semantic Layer & Model Tests   │    │   vectorized real-time OLAP & Async MVs  │    │
│  └──────────────────────────────────┘    └──────────────────────────────────────────┘    │
│                                                                   │                      │
│                                                                   ▼                      │
│  7. BUSINESS CONSUMERS: BI Dashboards · Reverse ETL · ML Stores · Customer Analytics     │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📚 Repository Structure & Learning Path

Each technology contains four structured guides following an engineering-first learning progression:

1. **`01_Overview.md`**: Core mission, historical genesis ("why was it created"), architecture overview, and ecosystem positioning.
2. **`02_Data_Guide.md`**: Internal data movement, memory/storage layout, serialization, and state handling.
3. **`03_Architecture.md`**: Clustering topology, master-worker coordination, scheduling, communication layers, and fault tolerance.
4. **`04_Performance_Guide.md`**: Memory allocation formulas, hardware planning, JVM/GC tuning, anti-patterns, and production deployment checklists.

---

## 📂 Technology Guides Directory

### 🚀 Stream Processing & Data Logistics
| Technology | 01 Overview | 02 Data Guide | 03 Architecture | 04 Performance Guide |
|---|---|---|---|---|
| **🔴 Apache Flink** | [01_Overview](Apache%20Flink/01_Overview.md) | [02_Data_Guide](Apache%20Flink/02_Data_Guide.md) | [03_Architecture](Apache%20Flink/03_Architecture.md) | [04_Performance_Guide](Apache%20Flink/04_Performance_Guide.md) |
| **⚫ Apache Kafka** | [01_Overview](Apache%20Kafka/01_Overview.md) | [02_Data_Guide](Apache%20Kafka/02_Data_Guide.md) | [03_Architecture](Apache%20Kafka/03_Architecture.md) | [04_Performance_Guide](Apache%20Kafka/04_Performance_Guide.md) |
| **🟢 Apache NiFi** | [01_Overview](Apache%20NiFi/01_Overview.md) | [02_Data_Guide](Apache%20NiFi/02_Data_Guide.md) | [03_Architecture](Apache%20NiFi/03_Architecture.md) | [04_Performance_Guide](Apache%20NiFi/04_Performance_Guide.md) |

### 📊 Transformation & Query Engines
| Technology | 01 Overview | 02 Data Guide | 03 Architecture | 04 Performance Guide |
|---|---|---|---|---|
| **🟠 dbt** | [01_Overview](dbt/01_Overview.md) | [02_Data_Guide](dbt/02_Data_Guide.md) | [03_Architecture](dbt/03_Architecture.md) | [04_Performance_Guide](dbt/04_Performance_Guide.md) |
| **🟣 Trino** | [01_Overview](Trino/01_Overview.md) | [02_Data_Guide](Trino/02_Data_Guide.md) | [03_Architecture](Trino/03_Architecture.md) | [04_Performance_Guide](Trino/04_Performance_Guide.md) |
| **🔵 StarRocks** | [01_Overview](StarRocks/01_Overview.md) | [02_Data_Guide](StarRocks/02_Data_Guide.md) | [03_Architecture](StarRocks/03_Architecture.md) | [04_Performance_Guide](StarRocks/04_Performance_Guide.md) |

### 🧊 Lakehouse Storage & Table Formats
| Technology | 01 Overview | 02 Data Guide | 03 Architecture | 04 Performance Guide |
|---|---|---|---|---|
| **🌊 Apache Paimon** | [01_Overview](Apache%20Paimon/01_Overview.md) | [02_Data_Guide](Apache%20Paimon/02_Data_Guide.md) | [03_Architecture](Apache%20Paimon/03_Architecture.md) | [04_Performance_Guide](Apache%20Paimon/04_Performance_Guide.md) |
| **🧊 Apache Iceberg** | [01_Overview](Apache%20Iceberg/01_Overview.md) | [02_Data_Guide](Apache%20Iceberg/02_Data_Guide.md) | [03_Architecture](Apache%20Iceberg/03_Architecture.md) | [04_Performance_Guide](Apache%20Iceberg/04_Performance_Guide.md) |
| **🐝 Apache Hive** | [01_Overview](Apache%20Hive/01_Overview.md) | [02_Data_Guide](Apache%20Hive/02_Data_Guide.md) | [03_Architecture](Apache%20Hive/03_Architecture.md) | [04_Performance_Guide](Apache%20Hive/04_Performance_Guide.md) |

---

## ⚖️ Architectural Deep-Dive Comparisons

In addition to individual technology guides, dedicated cross-engine comparison analyses are provided to guide platform architecture decisions:

| Comparison Document | Scope & Key Takeaways |
|---|---|
| **[🔍 Trino vs. StarRocks](Comparisons/Trino_vs_StarRocks.md)** | **Federated Data Lake SQL vs. Sub-Second Real-Time OLAP:**<br>• Java JVM bytecode compilation vs. C++ SIMD vectorization.<br>• Pure compute-only federation vs. Hybrid internal tables + NVMe data cache.<br>• Join optimization (Broadcast/Partitioned) vs. Cascades CBO with automated MV query rewrite.<br>• Decision matrix for ad-hoc exploration vs. high-QPS embedded dashboards. |
| **[🧊 Table Formats: Iceberg vs. Paimon vs. Hive](Comparisons/Table_Formats_Iceberg_vs_Paimon_vs_Hive.md)** | **The Evolution of Lakehouse Storage:**<br>• Directory-as-partition (Hive) vs. Snapshot Manifest Tree (Iceberg) vs. LSM-Tree on S3 (Paimon).<br>• Streaming CDC upserts and sub-minute commits (Paimon LSM flush vs. Iceberg MOR/COW).<br>• Hidden partitioning and schema evolution safety guarantees.<br>• Changelog generation: replacing intermediate Kafka topics with Paimon streaming feeds. |

---

## 🎯 Key Engineering Takeaways at a Glance

```
┌─────────────────┬────────────────────────────────────────────────────────────────────────┐
│ Technology      │ Golden Architectural Principle                                         │
├─────────────────┼────────────────────────────────────────────────────────────────────────┤
│ **Flink**       │ Batch is a special case of streaming; state is local, managed, and ABS.│
│ **Kafka**       │ Zero-copy sequential commit log; page cache over JVM heap allocation.  │
│ **NiFi**        │ Separate repositories onto distinct NVMe drives; prioritize records.   │
│ **dbt**         │ Software engineering for SQL; filter incrementally with lookback bounds│
│ **Trino**       │ Pipelined in-memory streaming execution; size Parquet files to 512MB.  │
│ **StarRocks**   │ C++ SIMD vectorization + Persistent Index on NVMe + Async MVs.         │
│ **Paimon**      │ LSM storage on S3 solves high-frequency streaming CDC upserts.         │
│ **Iceberg**     │ File-level metadata tracking eliminates slow S3 LIST operations (O(1)).│
│ **Hive**        │ Metastore (HMS) is the enduring catalog standard; migrate tables to ACID.│
└─────────────────┴────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Getting Started

Clone the repository to explore the markdown guides locally or contribute updates:

```bash
git clone https://github.com/ohkandil/big-data-knowledge-share.git
cd big-data-knowledge-share
```

---

## 🤝 Contributing

Contributions, fixes, benchmark updates, and new tool additions are welcome! Open an issue or submit a pull request following our structured 4-document framework.

---

## 📄 License

Distributed under the **MIT License**. See `LICENSE` for details.

<div align="center">
<br>

**⭐ Star this repository if it helped accelerate your data engineering knowledge! ⭐**

*Authored by [Omar Kandil](https://www.linkedin.com/in/omarhkandil) — Data Engineer @ Orange Egypt*

</div>
