---
title: "Apache NiFi: Performance Guide"
type: guide
tags:
  - apache-nifi
  - data-logistics
  - etl
  - ingestion
  - dataflow
  - flowfile
  - 04-performance-guide
aliases:
  - "NiFi Performance"
  - "NiFi Tuning"
  - "NiFi Back Pressure"
layer: "Data Logistics & Ingestion"
parent: "[[MOCs/MOC_Stream_Processing_and_Logistics]]"
---

# Apache NiFi: Performance Tuning & Development Best Practices

## Table of Contents
1. [Hardware & Infrastructure Planning](#1-hardware--infrastructure-planning)
2. [JVM Tuning & Memory Management](#2-jvm-tuning--memory-management)
3. [Repository Tuning: FlowFile, Content, Provenance](#3-repository-tuning-flowfile-content-provenance)
4. [Processor & Connection Optimization](#4-processor--connection-optimization)
5. [Network & Protocol Tuning](#5-network--protocol-tuning)
6. [Clustering & Load Balancing](#6-clustering--load-balancing)
7. [Record-Oriented Processing for Throughput](#7-record-oriented-processing-for-throughput)
8. [Backpressure & Queue Sizing Strategy](#8-backpressure--queue-sizing-strategy)
9. [Common Anti-Patterns & Pitfalls](#9-common-anti-patterns--pitfalls)
10. [Best Practices for Junior Data Engineers](#10-best-practices-for-junior-data-engineers)
11. [Monitoring & Diagnostics](#11-monitoring--diagnostics)
12. [References & Further Reading](#12-references--further-reading)

---

## 1. Hardware & Infrastructure Planning

### 1.1 Storage Hierarchy (Critical)
The single most impactful infrastructure decision is **repository isolation**. Each repository has a distinct I/O profile:

| Repository | I/O Pattern | Recommended Storage | Rationale |
|------------|-------------|---------------------|-----------|
| **FlowFile Repo** | High-frequency small random writes (WAL append) | Dedicated NVMe SSD, low latency | Synchronous commits on every FlowFile state change |
| **Content Repo** | Large sequential reads/writes (block streaming) | High-capacity NVMe SSD or RAID-0 array | Throughput oriented; stores actual payload bytes |
| **Provenance Repo** | Mixed: bulk index writes + point queries | Dedicated NVMe SSD | Lucene index segment merging competes with query I/O |

**Anti-Pattern:** Mounting all three on the same disk/volume. This causes I/O contention where large Content Repo writes starve FlowFile Repo WAL commits, leading to processor stalls and increased backpressure.

### 1.2 CPU & Memory Sizing
- **CPU:** NiFi is single-threaded per concurrent task. Size nodes for **8–16 vCPUs** to allow parallelism without contention. Prefer fewer, larger nodes over many small nodes (reduces ZooKeeper coordination overhead).
- **RAM:** Allocate **16–32 GB** for the JVM heap; additional OS page cache helps repository performance. For record-heavy workloads with large schemas, consider 32–64 GB.
- **Network:** 10 GbE minimum for cluster nodes; 25 GbE preferred when moving >500 MB/s between nodes via Site-to-Site.

### 1.3 Container/Kubernetes Considerations
- Use **StatefulSets** with **dedicated PersistentVolumeClaims** for each repository.
- Set `resources.limits.memory` ≥ 2× the JVM `-Xmx` to accommodate off-heap (direct buffers, Metaspace, Netty).
- Pin pods to nodes with local NVMe storage (`local-pv` or `hostPath`) for best I/O latency.

---

## 2. JVM Tuning & Memory Management

### 2.1 Heap Sizing
```bash
# In nifi-env.sh or via K8s env var NIFI_JAVA_HEAP_MAX
export JAVA_HEAP_MAX=16g
export JAVA_HEAP_MIN=16g
```
- **Rule:** Set `-Xms` = `-Xmx` (no dynamic heap expansion pauses).
- **Max Heap:** Do not exceed **32 GB** (CompressedOops limit). For larger memory needs, scale out horizontally.

### 2.2 Garbage Collector Selection
| Workload | Recommended GC | JVM Flags |
|----------|----------------|-----------|
| **General / Mixed** | **G1GC** (JDK 11+) | `-XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:G1HeapRegionSize=16M` |
| **High Throughput / Large Heap (24–32 GB)** | **ZGC** (JDK 17+) | `-XX:+UseZGC -XX:ConcGCThreads=4` |
| **Low Latency / Small Heap (< 8 GB)** | **Shenandoah** | `-XX:+UseShenandoahGC -XX:ShenandoahGCHeuristics=compact` |

**Avoid:** ParallelGC, CMS (deprecated).

### 2.3 Critical JVM Flags
```bash
# Disable explicit GC calls (some libraries call System.gc())
-XX:+DisableExplicitGC

# String deduplication (NiFi uses many repeated attribute strings)
-XX:+UseStringDeduplication

# Large page support (if OS configured)
-XX:+UseLargePages

# JIT compilation thresholds
-XX:CompileThreshold=1500

# Stack size (NiFi uses many threads)
-Xss512k
```

### 2.4 Off-Heap / Direct Memory
NiFi's Netty transport and NIO channels allocate **direct buffers** outside the heap.
```bash
# Reserve space for direct memory (Netty, MappedByteBuffers)
-XX:MaxDirectMemorySize=4g
```
Monitor via `DirectBufferPool` MBeans in JMX.

---

## 3. Repository Tuning: FlowFile, Content, Provenance

### 3.1 FlowFile Repository (WAL)
**Key Properties (`nifi.properties`):**
```properties
# Location - MUST be on dedicated fast SSD
nifi.flowfile.repository.directory=/mnt/nvme1/flowfile_repo

# Checkpoint interval (ms) - smaller = more frequent fsync, better recovery, more I/O
nifi.flowfile.repository.checkpoint.interval=20000  # 20s default

# Always sync on checkpoint - set false for higher throughput (risk of data loss on crash)
nifi.flowfile.repository.always.sync=true
```

**Tuning:**
- For **high-throughput pipelines** (>50k FlowFiles/s), increase checkpoint interval to 60–120s if you can tolerate potential replay.
- For **financial/critical data**, keep `always.sync=true` and interval at 20s.
- Monitor `FlowFileRepositoryCheckpoint` duration in logs; should be < 500ms.

### 3.2 Content Repository
```properties
# Dedicated high-throughput volume
nifi.content.repository.directory.default=/mnt/nvme2/content_repo

# Max claim size - larger claims = fewer files, less metadata overhead
nifi.content.claim.max.length=104857600  # 100 MB (default)

# Claim archive (recovery after crash) - disable if you can replay from source
nifi.content.repository.archive.enabled=false

# Encryption (if enabled) adds CPU overhead; use AES-NI hardware acceleration
nifi.content.repository.encryption.key.provider=FILE
nifi.content.repository.encryption.key=your-32-byte-key
```

**Large File Handling:**
- For files > 1 GB, increase `nifi.content.claim.max.length` to 1 GB to reduce claim fragmentation.
- Enable **Zero-Copy** where possible: `PutS3Object`, `FetchFile` use `java.nio.channels.FileChannel.transferTo()`.

### 3.3 Provenance Repository
```properties
# Dedicated volume
nifi.provenance.repository.directory.default=/mnt/nvme3/provenance_repo

# Implementation - Chronicle is faster for high write volume (NiFi 1.19+)
nifi.provenance.repository.implementation=org.apache.nifi.provenance.ChronicleProvenanceRepository

# Retention - balance audit requirements vs. disk usage
nifi.provenance.repository.max.storage.time=30 days
nifi.provenance.repository.max.storage.size=100 GB

# Index thread pool (if using Lucene)
nifi.provenance.repository.index.threads=4
```

**Chronicle vs. Lucene:**
- **Chronicle Provenance Repo** (recommended for >10k events/sec): Append-only, lock-free, no index merge storms. Query latency slightly higher but sustainable throughput is 5–10× Lucene.
- **Lucene Provenance Repo** (default): Rich querying (range, attribute search) but index merges cause periodic CPU/IO spikes.

---

## 4. Processor & Connection Optimization

### 4.1 Concurrent Tasks Configuration
Every processor has a **Concurrent Tasks** setting (default: 1). This controls intra-node parallelism.

| Processor Type | Recommended Concurrent Tasks |
|----------------|------------------------------|
| **Source (Polling):** `GetFile`, `ListSFTP`, `QueryDatabaseTable` | 1–2 (avoid overwhelming source) |
| **Source (Event):** `ConsumeKafka`, `ListenHTTP`, `ConsumeMQTT` | 4–16 (I/O bound; scale to partition count) |
| **Transform:** `ConvertRecord`, `UpdateRecord`, `QueryRecord` | 4–8 (CPU bound) |
| **Sink:** `PutDatabaseRecord`, `PutKafka`, `PutS3Object` | 4–16 (I/O bound; match downstream capacity) |
| **Routing:** `RouteOnAttribute`, `PartitionRecord` | 2–4 (lightweight) |

**Formula:** `Concurrent Tasks × Thread Pool Size ≤ Available CPU Cores`. Do not overcommit.

### 4.2 Processor Chaining vs. Connections
- **Chained Execution (Implicit):** When a processor feeds directly into another with no intermediate queue, NiFi can execute them in the **same thread** (no serialization, no queue overhead).
- **Explicit Connection:** Adds a queue (persistence, backpressure, prioritization). Use connections when:
  - You need backpressure.
  - You need to prioritize/reorder FlowFiles.
  - You need provenance separation between stages.

### 4.3 Avoiding Serialization Overhead
- Use **Record Processors** (`ConvertRecord`, `UpdateRecord`) instead of `SplitJson` → `EvaluateJsonPath` → `MergeContent`.
- Use **Avro** internally; schema is compact and serialization is fast.
- Avoid `AttributesToJSON` / `JSONToAttributes` for large payloads; they materialize entire content in heap.

---

## 5. Network & Protocol Tuning

### 5.1 Site-to-Site (S2S) Tuning
```properties
# Socket buffer sizes (increase for high throughput)
nifi.remote.input.socket.port=10443
nifi.remote.input.socket.send.buffer.size=2MB
nifi.remote.input.socket.receive.buffer.size=2MB

# Batch size for S2S transfers
nifi.remote.input.socket.batch.size=5000

# Compression (enable for WAN, disable for LAN)
nifi.remote.input.socket.compression.enabled=true
```

### 5.2 HTTP/S & REST
- **Jetty Thread Pool:** `nifi.web.server.threads=200` (default). Increase if UI/API latency spikes under load.
- **Idle Timeout:** `nifi.web.server.idle.timeout=30 sec` - aggressive timeouts free threads faster.
- **HTTPS:** Use **ECDHE** cipher suites with AES-GCM for hardware-accelerated TLS.

### 5.3 Kafka Integration (`ConsumeKafka_2_6` / `PublishKafka_2_6`)
```properties
# Consumer
Max Poll Records = 5000
Fetch Min Bytes = 1MB
Fetch Max Wait Ms = 500
Session Timeout Ms = 30000

# Producer
Batch Size = 65536
Linger Ms = 5
Buffer Memory = 64MB
Compression Type = SNAPPY (or ZSTD)
Acks = all
```

---

## 6. Clustering & Load Balancing

### 6.1 Cluster Sizing
- **Minimum 3 nodes** for ZooKeeper quorum.
- **5–7 nodes** optimal for production (balance failure domain vs. coordination overhead).
- Avoid > 10 nodes in a single cluster; split into multiple clusters with separate ZooKeeper ensembles.

### 6.2 Load Balancing Strategies
Configure on **Connections** (right-click → Configure → Load Balance Strategy):
| Strategy | Use Case |
|----------|----------|
| **Round Robin** | Homogeneous nodes, stateless processing |
| **Single Node** | Stateful processor (e.g., single-threaded DB writer) |
| **Partition by Attribute** | Hash-based partitioning on key (e.g., `user.id`) |

**For Kafka sources:** Use `ConsumeKafka` with **Group ID** set to same value across cluster; Kafka handles partition assignment automatically.

### 6.3 Primary Node Pattern
For processors that must run on **exactly one node** (e.g., `ExecuteSQL` for DDL, `ListSFTP` to avoid duplicate file listing):
- Set **Scheduling Strategy** = `Primary Node Only`.
- NiFi ensures only the designated primary node runs the task.
- If primary fails, another node is elected automatically.

---

## 7. Record-Oriented Processing for Throughput

### 7.1 Why Record Processors Win
| Pattern | FlowFiles Created | Provenance Events | Disk I/O |
|---------|-------------------|-------------------|----------|
| Split → Transform → Merge | 10,000 | 30,000+ | Very High |
| **Record Processor (Single Pass)** | **1** | **1** | **Low** |

### 7.2 Essential Record Processor Toolkit
| Processor | Use Case |
|-----------|----------|
| **ConvertRecord** | Format conversion (CSV ↔ JSON ↔ Avro ↔ Parquet) |
| **UpdateRecord** | Field-level updates using RecordPath (e.g., hash PII, add timestamp) |
| **QueryRecord** | SQL-like filtering/aggregation on record streams (Calcite) |
| **PartitionRecord** | Split single FlowFile into multiple by field value |
| **MergeRecord** | Combine multiple FlowFiles into one (respects schema) |

### 7.3 Schema Registry Performance
- **Cache schemas locally:** `AvroSchemaRegistry` caches in memory; avoid remote HTTP calls per record.
- **Pre-register schemas:** Do not use "Infer Schema" in production; it adds latency and instability.

---

## 8. Backpressure & Queue Sizing Strategy

### 8.1 Sizing Methodology
**Goal:** Survive downstream outage of **T minutes** without data loss.

```
Queue Size (bytes) ≥ Ingest Rate (bytes/sec) × Outage Tolerance (sec)
Queue Count    ≥ Ingest Rate (FlowFiles/sec) × Outage Tolerance (sec)
```

**Example:** Ingest 10 MB/s, tolerate 30 min outage → Queue Size ≥ 18 GB. Set thresholds at 70% (e.g., 12 GB / 8k objects) to trigger backpressure early.

### 8.2 Backpressure Configuration
```properties
# On Connection → Configure → Backpressure
# Object Threshold: 10000 (or calculated)
# Data Size Threshold: 1 GB (or calculated)

# Global default (can be overridden per connection)
nifi.queue.backpressure.object.count=10000
nifi.queue.backpressure.data.size=1 GB
```

### 8.3 Prioritization for SLA
- **High-Priority FlowFiles:** Set `priority` attribute (integer, lower = higher priority).
- Use `PriorityAttributePrioritizer` on critical paths (e.g., fraud alerts).
- **Caution:** Prioritization adds sorting overhead; only enable where SLA demands it.

---

## 9. Common Anti-Patterns & Pitfalls

| Anti-Pattern | Symptom | Fix |
|--------------|---------|-----|
| **Split → Transform → Merge for record data** | JVM OOM, slow checkpoints, UI lag | Use `ConvertRecord` / `UpdateRecord` / `QueryRecord` |
| **Storing large content in Attributes** | Heap OOM, GC thrashing | Keep attributes small; use Content for payloads > 1 KB |
| **No repository isolation** | High I/O wait, random processor stalls | Separate disks for FlowFile, Content, Provenance |
| **Over-provisioning Concurrent Tasks** | Context switch storm, CPU saturation | Match concurrent tasks to CPU cores / I/O profile |
| **Unbounded Provenance Repo** | Disk full, NiFi crash | Set `max.storage.size` and `max.storage.time` |
| **Single-node cluster with no HA** | Data loss on node failure | Minimum 3 nodes + ZooKeeper ensemble |
| **Using `ExecuteScript` for heavy logic** | Slow, hard to maintain, no schema | Write custom Java Processor or use `QueryRecord` |
| **Ignoring SSL/TLS overhead** | High CPU on encryption | Use AES-NI, session resumption, or disable on trusted LAN |
| **No monitoring/alerting** | Silent failures, backpressure unnoticed | Export JMX → Prometheus → Grafana alerts |

---

## 10. Best Practices for Junior Data Engineers

### 10.1 Development Workflow
1. **Design in NiFi Registry:** Version control flows via **NiFi Registry** (Git-backed). Promote flows across Dev → Test → Prod.
2. **Parameterize Everything:** Use **Parameter Contexts** for environment-specific values (hosts, ports, credentials). Never hardcode.
3. **Test Locally with MiniFi:** Use **Apache MiNiFi** (C++/Java agent) for edge testing; same processor logic, smaller footprint.
4. **Use Flow Templates:** Export/Import `.xml` templates for reusable sub-flows (e.g., "Standard Kafka Ingest", "CDC to Iceberg").

### 10.2 Building Robust Flows
- **Error Handling:** Every processor must have `failure` and `retry` relationships connected.
  - `failure` → Log + Route to Dead Letter Queue (DLQ) topic in Kafka or S3 prefix.
  - `retry` → Penalize (`PenalizeFlowFile` processor) → Loop back with exponential backoff.
- **Idempotency:** Design flows to be idempotent. Use `DetectDuplicate` with Distributed Cache for at-least-once sources.
- **Schema Contracts:** Define schemas in `AvroSchemaRegistry`; enforce with `ValidateRecord` before sink.

### 10.3 Deployment Checklist
- [ ] **Repositories on separate volumes** (verified with `iostat -x 1`).
- [ ] **JVM heap set** (Xms=Xmx, ≤32GB, G1GC/ZGC configured).
- [ ] **ZooKeeper ensemble** (3/5 nodes, separate disks).
- [ ] **Backpressure thresholds** calculated and set on all connections.
- [ ] **Provenance retention** configured (disk won't fill).
- [ ] **SSL/TLS** configured for all external endpoints.
- [ ] **RBAC policies** applied (least privilege).
- [ ] **Metrics exported** (Prometheus reporter enabled).
- [ ] **Alerting rules** for: backpressure, checkpoint duration, disk usage, JVM GC pause.
- [ ] **Backup strategy** for `flow.xml.gz` and repo snapshots.

---

## 11. Monitoring & Diagnostics

### 11.1 Key Metrics (Expose via Prometheus)
| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| `nifi_flowfile_repository_checkpoint_duration_seconds` | JMX | > 5s |
| `nifi_content_repository_usage_bytes` | JMX | > 80% capacity |
| `nifi_provenance_repository_usage_bytes` | JMX | > 80% capacity |
| `nifi_processor_flowfiles_in_5min` | JMX | Sudden drop to 0 |
| `nifi_processor_process_duration_millis_p99` | JMX | > 10s |
| `nifi_queue_bytes` / `nifi_queue_count` | JMX | > 70% backpressure threshold |
| `jvm_memory_heap_used_bytes` / `jvm_gc_pause_seconds_max` | Micrometer | Heap > 85%, GC pause > 1s |

### 11.2 Debugging Tools
- **NiFi UI:** Backpressure view, Queue listing, Provenance search.
- **`nifi.sh status` / `dump`**: Thread dumps, heap histograms.
- **`jcmd <pid> GC.heap_info`**: Live heap analysis.
- **`nifi-toolkit` CLI:** `flow-analyzer`, `encrypt-config`, `file-manager`.

### 11.3 Log Analysis
```bash
# Key log patterns to grep
grep -i "backpressure" nifi-app.log
grep -i "checkpoint" nifi-app.log | grep -i "took"
grep -i "OutOfMemoryError" nifi-app.log
grep -i "RejectedExecutionException" nifi-app.log  # Thread pool exhaustion
```

---

## 12. References & Further Reading

1. **NiFi System Administrator's Guide:** https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html
2. **NiFi Performance Tuning (Community Wiki):** https://cwiki.apache.org/confluence/display/NIFI/Performance+Tuning
3. **JVM Tuning for NiFi:** https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#jvm-tuning
4. **NiFi Record Processing Guide:** https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#record-processing
5. **NiFi Clustering & HA:** https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#cluster-setup
6. **Apache MiNiFi (Edge Agent):** https://nifi.apache.org/minifi/
7. **NiFi Registry (Version Control):** https://nifi.apache.org/registry/
8. **Prometheus JMX Exporter for NiFi:** https://github.com/prometheus/jmx_exporter
9. **Flink + NiFi Integration Patterns:** https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/nifi/
10. **NiFi Developer's Guide (Custom Processors):** https://nifi.apache.org/docs/nifi-docs/html/developer-guide.html

---

**Layer:** 🟢 Data Logistics & Ingestion  
**Parent MOC:** [[MOCs/MOC_Stream_Processing_and_Logistics|MOC: Stream Processing & Logistics]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** ← [[Apache NiFi/03_Architecture|Architecture]]

**Related technologies:** [[Apache Kafka/01_Overview|Apache Kafka]] · [[Apache Flink/01_Overview|Apache Flink]]
