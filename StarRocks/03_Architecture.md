# StarRocks: Runtime Architecture & Clustering Topology

## Table of Contents
1. [Frontend (FE) & Backend (BE) Cluster Topology](#1-frontend-fe--backend-be-cluster-topology)
2. [Shared-Nothing vs. Shared-Data Architecture](#2-shared-nothing-vs-shared-data-architecture)
3. [The Vectorized Pipeline Execution Engine](#3-the-vectorized-pipeline-execution-engine)
4. [The Cascades Cost-Based Optimizer (CBO)](#4-the-cascades-cost-based-optimizer-cbo)
5. [Local NVMe Data Cache Architecture](#5-local-nvme-data-cache-architecture)
6. [High Availability & Metadata Consensus (BDB-JE Raft)](#6-high-availability--metadata-consensus-bdb-je-raft)
7. [References & Further Reading](#7-references--further-reading)

---

## 1. Frontend (FE) & Backend (BE) Cluster Topology

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   STARROCKS CLUSTER                                     │
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐  │
│  │                              FRONTEND (FE) ENSEMBLE                               │  │
│  │  ┌────────────────────────┐  ┌─────────────────────────┐  ┌────────────────────┐  │  │
│  │  │ FE Leader (R/W Master) │  │ FE Follower (Standby)   │  │ FE Observer (R/O)  │  │  │
│  │  │ • Metadata Mutations   │◄─┼─► State Sync (BDB-JE)   │◄─┼─► Scale Read QPS   │  │  │
│  │  │ • CBO Plan Generation  │  │ • High Availability     │  │ • Client Connectors│  │  │
│  │  └────────────────────────┘  └─────────────────────────┘  └────────────────────┘  │  │
│  └─────────────────────────────────────────┬─────────────────────────────────────────┘  │
│                                            │ Dispatches Plan Fragments                  │
│                                            ▼                                            │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐  │
│  │                               BACKEND (BE) POOL                                   │  │
│  │  ┌──────────────────────────────┐                ┌──────────────────────────────┐ │  │
│  │  │ Backend (BE) Node 1          │                │ Backend (BE) Node 2          │ │  │
│  │  │  • Pipeline Execution Engine │◄──DataShuffle─►│  • Pipeline Execution Engine │ │  │
│  │  │  • Vectorized SIMD Operators │                │  • Vectorized SIMD Operators │ │  │
│  │  │  • Persistent Tablets (Disk) │                │  • Persistent Tablets (Disk) │ │  │
│  │  │  • NVMe Lakehouse Cache      │                │  • NVMe Lakehouse Cache      │ │  │
│  │  └──────────────────────────────┘                └──────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Shared-Nothing vs. Shared-Data Architecture

StarRocks supports two deployment architectures:

| Architectural Model | Data Storage Location | Compute Scaling | Best For |
| :--- | :--- | :--- | :--- |
| **Shared-Nothing (Classic)** | Distributed across local SSD/NVMe drives on BE nodes | Scale compute and storage together by adding BE nodes | On-premise bare-metal, ultra-low latency requirements |
| **Shared-Data (Cloud-Native)** | Persistent data resides on object storage (AWS S3, Azure ADLS, GCS) | Compute nodes (CN) are completely stateless and scale elastically | Cloud deployments, variable workloads, cost optimization |

In **Shared-Data** mode, stateless Compute Nodes (CN) use local ephemeral NVMe disks solely as a cache, allowing compute to scale from 5 to 50 nodes in seconds during peak business hours.

---

## 3. The Vectorized Pipeline Execution Engine

StarRocks completely abandons the traditional "one thread per query" model in favor of an **asynchronous coroutine-driven Pipeline Engine**:

- **Pipeline Driver:** Small execution fragments executing non-blocking work chunks on CPU worker threads.
- **Dynamic Work Stealing:** If a task blocks on I/O (e.g., waiting for an S3 network packet), the CPU worker thread instantly context-switches to another ready pipeline without OS thread context-switch penalties.
- **Resource Group Isolation:** Queries are assigned CPU and memory quotas to guarantee SLA for mission-critical dashboards.

---

## 4. The Cascades Cost-Based Optimizer (CBO)

StarRocks implements an advanced **Cascades-style CBO** framework:
- **Join Graph Search Space:** Explores Left-Deep, Right-Deep, and Bushy join trees across dozens of joined tables.
- **Runtime Filter Generation:** Dynamically creates Bloom filters, In-filters, and Min/Max range filters at join build nodes and pushes them down to storage scan threads in real time.
- **CTE Optimization:** Detects common subqueries and materializes them in memory once for multi-consumer reuse.

---

## 5. Local NVMe Data Cache Architecture

When querying external data lakes (Iceberg/Paimon on S3):
1. **Cache Blocks:** S3 data files are split into fixed 1MB–8MB cache blocks on local NVMe SSDs.
2. **Consistent Hashing:** Queries route scan ranges to specific BE nodes based on consistent hashing of S3 file paths.
3. **Sub-Millisecond Hit Rate:** Hot data hits local NVMe cache at 2–3 GB/s, completely eliminating cloud object storage latency.

---

## 6. High Availability & Metadata Consensus (BDB-JE Raft)

- **FE High Availability:** FE nodes form a quorum (typically 1 Leader + 2 Followers).
- **Metadata Persistence:** Metadata changes (table creation, schema changes, tablet locations) are committed to the BDB-JE Raft log.
- **Failover:** If the FE Leader crashes, the surviving Followers elect a new Leader within 3 seconds, ensuring continuous availability.

---

## 7. References & Further Reading

1. **StarRocks Shared-Data Architecture:** [https://docs.starrocks.io/docs/architecture/shared_data/](https://docs.starrocks.io/docs/architecture/shared_data/)
2. **Pipeline Engine Internals:** [https://docs.starrocks.io/docs/architecture/pipeline_engine/](https://docs.starrocks.io/docs/architecture/pipeline_engine/)
3. **StarRocks Data Cache Guide:** [https://docs.starrocks.io/docs/data_source/data_cache/](https://docs.starrocks.io/docs/data_source/data_cache/)
