# Trino: Architecture & Distributed Execution Topology

## Table of Contents
1. [Master-Worker Architecture: Coordinator & Workers](#1-master-worker-architecture-coordinator--workers)
2. [Query Lifecycle: Parser, Analyzer, Optimizer, Planner](#2-query-lifecycle-parser-analyzer-optimizer-planner)
3. [Physical Execution Graph: Stages, Tasks, Pipelines, Drivers](#3-physical-execution-graph-stages-tasks-pipelines-drivers)
4. [In-Memory Shuffle & The Exchange Operator](#4-in-memory-shuffle--the-exchange-operator)
5. [The Cost-Based Optimizer (CBO) & Dynamic Filtering](#5-the-cost-based-optimizer-cbo--dynamic-filtering)
6. [Fault-Tolerant Execution Architecture (Project Tardigrade)](#6-fault-tolerant-execution-architecture-project-tardigrade)
7. [Cluster Deployment & High Availability](#7-cluster-deployment--high-availability)
8. [References & Further Reading](#8-references--further-reading)

---

## 1. Master-Worker Architecture: Coordinator & Workers

Trino consists of a **Coordinator** node and an arbitrary number of **Worker** nodes connected over a local network.

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   TRINO COORDINATOR                                     │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌───────────────────────────────┐  │
│  │ Statement Resource   │  │ Query Planner & CBO  │  │ Split & Node Scheduler        │  │
│  │ (REST / WebUI / Auth)│  │ (Transforms AST->DAG)│  │ (Assigns Tasks to Workers)    │  │
│  └──────────────────────┘  └──────────────────────┘  └───────────────────────────────┘  │
└────────────────────────────────────────┬────────────────────────────────────────────────┘
                                         │ RPC / Task Scheduling
        ┌────────────────────────────────┼────────────────────────────────┐
        ▼                                ▼                                ▼
┌──────────────────────────────┐ ┌──────────────────────────────┐ ┌───────────────────────┐
│        TRINO WORKER 1        │ │        TRINO WORKER 2        │ │     TRINO WORKER 3    │
│  ┌────────────────────────┐  │ │  ┌────────────────────────┐  │ │ ┌───────────────────┐ │
│  │ Task Execution Engine  │  │ │  │ Task Execution Engine  │  │ │ │ Task Engine       │ │
│  │ (Pipelined Drivers)    │  │ │  │ (Pipelined Drivers)    │  │ │ │ (Pipelined Drivers│ │
│  └───────────┬────────────┘  │ │  └───────────┬────────────┘  │ │ └─────────┬─────────┘ │
│              │               │ │              │               │ │           │           │
│  ┌───────────▼────────────┐  │ │  ┌───────────▼────────────┐  │ │ ┌─────────▼─────────┐ │
│  │ ExchangeClient / Buffer│◄─┼─┼─►│ ExchangeClient / Buffer│◄─┼─┼►│ ExchangeClient/Buf│ │
│  └────────────────────────┘  │ │  └────────────────────────┘  │ │ └───────────────────┘ │
└──────────────────────────────┘ └──────────────────────────────┘ └───────────────────────┘
```

---

## 2. Query Lifecycle: Parser, Analyzer, Optimizer, Planner

When a SQL query is submitted:

```
[ SQL String ] ──► [ ANTLR Parser ] ──► [ Semantic Analyzer ] ──► [ Logical Planner ]
                                                                        │
                                                                        ▼
                                                           [ Cost-Based Optimizer ]
                                                                        │
                                                                        ▼
                                                           [ Distributed Execution Plan ]
```

1. **Parsing:** ANTLR grammar parses raw SQL into an Abstract Syntax Tree (AST).
2. **Semantic Analysis:** Validates table names, column existence, permissions, and resolves data types against the catalog connector.
3. **Logical Planning:** Converts AST into a relational algebra plan (Tree of PlanNodes: Scan, Filter, Project, Join, Aggregate).
4. **Optimization:** Cost-Based Optimizer (CBO) uses table statistics (row count, distinct values, null fraction) to reorder joins, push down predicates, and prune columns.
5. **Physical Scheduling:** Divides the plan into distributed **Stages** and schedules tasks on workers.

---

## 3. Physical Execution Graph: Stages, Tasks, Pipelines, Drivers

Trino decomposes a distributed query into a 4-tier hierarchy:

```
[ Stage 0: Root Final Output ]
             ▲ (Exchange)
[ Stage 1: Distributed Hash Aggregation / Join ]
             ▲ (Exchange)
[ Stage 2: Parallel Source Table Scans (Splits) ]
```

- **Stage:** A major execution phase (e.g., source scan stage vs. aggregation stage). Stages communicate via Exchanges.
- **Task:** A parallel instance of a stage assigned to a specific worker machine.
- **Pipeline:** A sequence of connected operators within a task (e.g., `ScanFilterAndProjectOperator -> HashBuilderOperator`).
- **Driver:** The actual unit of parallel execution inside a worker. A driver runs on an operating system thread, processing Pages through a pipeline.

---

## 4. In-Memory Shuffle & The Exchange Operator

When data must be redistributed across workers (e.g., partitioning by key for a `GROUP BY` or `JOIN`), Trino uses the **Exchange Operator**:

- **ExchangeSink:** The upstream worker hashes the join/grouping key of each row and pushes Pages into partitioned in-memory network buffers.
- **ExchangeClient:** Downstream workers issue non-blocking HTTP GET requests to fetch Pages directly from the upstream worker's in-memory buffers.
- **Zero Disk Writes:** In default execution mode, data never touches the disk during a shuffle.

---

## 5. The Cost-Based Optimizer (CBO) & Dynamic Filtering

### Cost-Based Join Reordering
A `JOIN B ON A.id = B.id`:
- **Broadcast Join:** Replicates the small build table $B$ to every worker. Scans large probe table $A$ locally.
- **Distributed Partitioned Join:** Hashes both $A$ and $B$ across the network on `id`. Used when both tables are massive.
- The CBO automatically chooses Broadcast vs. Partitioned joins based on connector table statistics.

### Dynamic Filtering
When joining a massive fact table with a filtered dimension table:
1. Trino builds a Bloom filter or hash set of qualifying keys from the small dimension table during stage execution.
2. The Coordinator broadcasts this dynamic filter directly to the workers scanning the large fact table.
3. The fact table scan uses this filter to skip reading matching Parquet files from S3 before they are even transferred over the network.

---

## 6. Fault-Tolerant Execution Architecture (Project Tardigrade)

Historically, if a single worker crashed during a 30-minute Trino query, the entire query failed and had to be restarted from scratch.

With **Fault-Tolerant Execution (Exchange Spooling)**:
- Intermediate exchange data between stages is spooled to durable storage (S3, MinIO, or worker local NVMe disks).
- If a worker crashes, the Coordinator detects the failure, reallocates the failed task to another healthy worker, and replays the upstream stage output without restarting the entire query.
- Enables Trino to run massive, multi-hour batch ETL queries reliably.

---

## 7. Cluster Deployment & High Availability

- **Kubernetes Native (Trino on K8s):** Deployed via Helm charts or the Trino Kubernetes Operator. Workers scale dynamically using Horizontal Pod Autoscalers (HPA).
- **Coordinator HA:** Multiple coordinators configured with an external load balancer and ZooKeeper/Raft leader election.

---

## 8. References & Further Reading

1. **Trino Architecture Concepts:** [https://trino.io/docs/current/overview/concepts.html](https://trino.io/docs/current/overview/concepts.html)
2. **Trino Cost-Based Optimizer:** [https://trino.io/docs/current/optimizer/cost-based-optimizations.html](https://trino.io/docs/current/optimizer/cost-based-optimizations.html)
3. **Trino Fault-Tolerant Execution:** [https://trino.io/docs/current/admin/fault-tolerant-execution.html](https://trino.io/docs/current/admin/fault-tolerant-execution.html)
