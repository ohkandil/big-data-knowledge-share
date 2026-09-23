<div align="center">

# 🏗️ Big Data Knowledge Share

### Deep-dive engineering guides for the modern streamhouse stack

**Apache Flink · Apache Kafka · Apache NiFi**

[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-e6526f?style=for-the-badge&logo=apache%20flink&logoColor=white)](https://flink.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231f20?style=for-the-badge&logo=apache%20kafka&logoColor=white)](https://kafka.apache.org/)
[![Apache NiFi](https://img.shields.io/badge/Apache%20NiFi-618c39?style=for-the-badge&logo=apache%20nifi&logoColor=white)](https://nifi.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)](LICENSE)

*Companion guides written for data engineers who want to understand **how these systems actually work** — not just how to use them.*

</div>

---

## 📚 What's Inside

Four structured guides per technology, following the same learning path: **overview → data model → internals → performance**.

| Guide | What You'll Learn |
|---|---|
| **[01 Overview](#guides)** | Core mission, historical genesis ("why it was created"), positioning in the modern lakehouse/streamhouse stack, and comparisons vs. competing engines |
| **[02 Data Guide](#guides)** | The fundamental data abstractions — FlowFiles, topics/partitions, streams & state — and how each engine models, stores, and moves data |
| **[03 Architecture](#guides)** | Internal topology: masters & workers, consensus (KRaft, zero-master clustering, JobManager), replication, deployment modes, and HA strategies |
| **[04 Performance Guide](#guides)** | Battle-tested tuning: JVM/GC, memory tiers, network stacks, anti-patterns, and production deployment checklists |

---

## 🧭 Where These Fit in Your Stack

```text
┌─────────────────────────────────────────────────────────────────┐
│  SOURCES: Microservices · IoT · CDC · Logs · Files               │
└──────────────┬──────────────────────────────────────────────────┘
               ▼
      🟢 APACHE NIFI          Data logistics: ingest, validate, route,
      (Data Logistics)        track provenance across 300+ protocols
               ▼
      ⚫ APACHE KAFKA          The event backbone: immutable, partitioned,
      (Event Streaming)       replicated commit log — millions of events/sec
               ▼
      🔴 APACHE FLINK          Stateful compute: windowing, joins, CEP,
      (Stream Processing)      exactly-once guarantees, sub-10ms latency
               ▼
┌─────────────────────────────────────────────────────────────────┐
│  STREAMHOUSE: Apache Iceberg / Paimon on S3 · Trino · StarRocks  │
└─────────────────────────────────────────────────────────────────┘
```

Each guide shows exactly how its technology plugs into this topology — and where its competitors (Pulsar, RabbitMQ, Spark, Airbyte, Logstash) are the better choice.

---

## 📂 The Guides

<details open>
<summary><b>🔴 Apache Flink</b> — Distributed Stateful Stream Processing</summary>

| Document | Contents |
|---|---|
| [01_Overview](Apache%20Flink/01_Overview.md) | True streaming vs. micro-batch, Flink's genesis, runtime processes (JobManager / TaskManager) |
| [02_Data_Guide](Apache%20Flink/02_Data_Guide.md) | Time semantics (event/process time), watermarks, state backends, checkpointing |
| [03_Architecture](Apache%20Flink/03_Architecture.md) | JobManager internals (Dispatcher, ResourceManager, JobMaster), slot sharing, network stack, K8s/YARN deployment |
| [04_Performance_Guide](Apache%20Flink/04_Performance_Guide.md) | Memory model tuning, RocksDB state optimization, unaligned checkpoints, data-skew fixes, anti-patterns |

</details>

<details open>
<summary><b>⚫ Apache Kafka</b> — Distributed Event Streaming Platform</summary>

| Document | Contents |
|---|---|
| [01_Overview](Apache%20Kafka/01_Overview.md) | The commit-log paradigm, LinkedIn's origins, pull-based consumption, KRaft vs. ZooKeeper |
| [02_Data_Guide](Apache%20Kafka/02_Data_Guide.md) | Topics, partitions, replication, ISR, consumer groups, offsets, compaction |
| [03_Architecture](Apache%20Kafka/03_Architecture.md) | Broker internals, KRaft Raft quorum, segment storage, zero-copy I/O, rack awareness |
| [04_Performance_Guide](Apache%20Kafka/04_Performance_Guide.md) | Broker/producer/consumer tuning, JVM sizing, compression, monitoring, security |

</details>

<details open>
<summary><b>🟢 Apache NiFi</b> — Automated Dataflow & Data Logistics</summary>

| Document | Contents |
|---|---|
| [01_Overview](Apache%20NiFi/01_Overview.md) | NSA origins, Flow-Based Programming, FlowFile abstraction, three-repository architecture |
| [02_Data_Guide](Apache%20NiFi/02_Data_Guide.md) | Content immutability, FlowFile/Content/Provenance repos, record-oriented processing |
| [03_Architecture](Apache%20NiFi/03_Architecture.md) | Zero-master clustering, thread pools, controller services, backpressure, RBAC & encryption |
| [04_Performance_Guide](Apache%20NiFi/04_Performance_Guide.md) | Repository isolation on NVMe, GC tuning (G1/ZGC), record processor throughput, queue sizing math |

</details>

---

## ✨ What Makes These Different

- **First-principles depth** — Why does Kafka use `sendfile()`? Why are NiFi FlowFiles immutable? Why does Flink use a variant of Chandy-Lamport? The *why* behind every design decision.
- **Stack-aware** — Every guide positions its technology against real alternatives (Pulsar, RabbitMQ, Spark, Kinesis, Airbyte, dbt) with honest trade-off tables.
- **Production-grade** — Deployment checklists, anti-pattern tables with symptom → fix mappings, alerting thresholds, and sizing formulas you can use on day one.
- **Written for the streamhouse era** — Kafka + Flink + NiFi + Iceberg/Paimon, not the Hadoop-era stack.

---

## 🚀 Getting Started

Pick your poison:

1. **New to a technology?** Read its `01_Overview.md` first — genesis story and ecosystem positioning.
2. **Building a pipeline?** Jump to the `04_Performance_Guide.md` for the deployment checklist.
3. **Debugging production?** Check the anti-pattern tables and monitoring sections in each performance guide.

No build step, no dependencies — just Markdown. Clone and read:

```bash
git clone https://github.com/ohkandil/big-data-knowledge-share.git
```

---

## 📈 Coming Next

- [ ] Apache Iceberg — table format internals (manifests, compaction, time travel)
- [ ] Apache Paimon — streaming-native lake storage
- [ ] Apache Spark — batch & structured streaming
- [ ] dbt & Trino — the serving and modeling layer

---

## 🤝 Contributing

Found an error, an outdated default, or a better tuning recipe? PRs and issues are welcome — these are living documents.

---

## 📄 License

MIT — use them, share them, teach with them.

<div align="center">
<br>

**⭐ Star this repo if it helped you level up. ⭐**

*Written by [Omar Kandil](https://www.linkedin.com/in/omarhkandil) — Data Engineer @ Orange Egypt*

</div>
