# Apache Hive: Architecture & Engine Topology

## Table of Contents
1. [Core Daemon Architecture: HS2 & HMS](#1-core-daemon-architecture-hs2--hms)
2. [The Hive Metastore (HMS) Schema & Database Layout](#2-the-hive-metastore-hms-schema--database-layout)
3. [Execution Engines: Tez, MapReduce, LLAP](#3-execution-engines-tez-mapreduce-llap)
4. [Query Compilation & Cost-Based Optimizer (Apache Calcite)](#4-query-compilation--cost-based-optimizer-apache-calcite)
5. [Locking Mechanisms: ZooKeeper vs. Hive Metastore Locks](#5-locking-mechanisms-zookeeper-vs-hive-metastore-locks)
6. [References & Further Reading](#6-references--further-reading)

---

## 1. Core Daemon Architecture: HS2 & HMS

```
┌─────────────────────────────────────────────────────────────────────────┐
│                               APACHE HIVE                               │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ HiveServer2 (HS2)                                                 │  │
│  │ • Thrift Service (Port 10000) for JDBC / ODBC / Beeline Clients   │  │
│  │ • SQL Parser, Semantic Analyzer, Calcite Cost-Based Optimizer     │  │
│  │ • Submits DAG execution plans to Apache Tez / YARN                │  │
│  └─────────────────────────────────┬─────────────────────────────────┘  │
│                                    │ Metadata RPC (Port 9083)           │
│                                    ▼                                    │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ Hive Metastore (HMS)                                              │  │
│  │ • Thrift Service handling table schemas, partitions, stats        │  │
│  │ • Backed by relational DB (MySQL / PostgreSQL / Oracle)           │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. The Hive Metastore (HMS) Schema & Database Layout

HMS stores metadata in relational tables:
- **`DBS`:** Database names and filesystem root locations.
- **`TBLS`:** Table definitions, types (`MANAGED_TABLE`, `EXTERNAL_TABLE`), and SerDe references.
- **`PARTITIONS`:** Partition names and values (`dt=2025-01-15`).
- **`SDS`:** Storage descriptors containing physical directory URIs, input/output formats, and bucket counts.
- **`TAB_COL_STATS` / `PART_COL_STATS`:** Column-level statistics (min, max, nulls, distinct counts) for the Cost-Based Optimizer.

---

## 3. Execution Engines: Tez, MapReduce, LLAP

```
[ MapReduce (Legacy) ] ──► Disk-bound 2-stage execution (Map -> Disk -> Reduce)
           │
           ▼
[ Apache Tez (Modern) ] ──► In-memory streaming DAG (Vertices + Edges without disk write)
           │
           ▼
[ Hive LLAP (Hybrid) ]  ──► Long-running daemon workers with in-memory ORC cache & async I/O
```

- **Tez DAG Execution:** Eliminates artificial map-reduce boundaries. Complex multi-join queries run as a single pipelined DAG.
- **LLAP (Live Long and Process):** Stateless daemon workers running on Hadoop worker nodes caching columnar ORC stripes in off-heap memory, executing queries in sub-seconds.

---

## 4. Query Compilation & Cost-Based Optimizer (Apache Calcite)

1. **Parser:** Compiles HiveQL into AST.
2. **Semantic Analyzer:** Resolves table names against HMS.
3. **Apache Calcite Optimizer:**
   - Join reordering (Star join optimization).
   - Predicate pushdown into storage readers.
   - Partition pruning and point projection elimination.
4. **Physical Plan Generator:** Converts optimized logical plan into a Tez DAG.

---

## 5. Locking Mechanisms: ZooKeeper vs. Hive Metastore Locks

- **Hive Metastore DB Locks (Default):** Uses transactions inside the HMS backing database (MySQL/Postgres) to acquire table and partition read/write locks.
- **ZooKeeper Locks (Legacy):** Uses ephemeral z-nodes to coordinate locks.

---

## 6. References & Further Reading

1. **Hive Architecture Design:** [https://cwiki.apache.org/confluence/display/Hive/Design](https://cwiki.apache.org/confluence/display/Hive/Design)
2. **Hive Metastore Schema Internals:** [https://cwiki.apache.org/confluence/display/Hive/Hive+Schema](https://cwiki.apache.org/confluence/display/Hive/Hive+Schema)
3. **Apache Tez Integration:** [https://tez.apache.org/](https://tez.apache.org/)
