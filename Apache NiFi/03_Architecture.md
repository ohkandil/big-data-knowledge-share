# Apache NiFi: Architecture & Deployment Topology

## Table of Contents
1. [High-Level System Architecture](#1-high-level-system-architecture)
2. [Cluster Coordination & Zero-Master Model](#2-cluster-coordination--zero-master-model)
3. [Core Runtime Engine: Flow Controller & Scheduling](#3-core-runtime-engine-flow-controller--scheduling)
4. [Repositories: FlowFile, Content, Provenance](#4-repositories-flowfile-content-provenance)
5. [Processor Execution Model & Thread Pools](#5-processor-execution-model--thread-pools)
6. [Controller Services & Shared Resources](#6-controller-services--shared-resources)
7. [Connections, Queues, and Prioritizers](#7-connections-queues-and-prioritizers)
8. [Security Model: TLS, RBAC, and Encryption at Rest](#8-security-model-tls-rbac-and-encryption-at-rest)
9. [Deployment Modes: Standalone, Clustered, and Cloud Native](#9-deployment-modes-standalone-clustered-and-cloud-native)
10. [High Availability (HA) Strategies](#10-high-availability-ha-strategies)
11. [Extensibility: Custom Processors & Extensions](#11-extensibility-custom-processors--extensions)
12. [Monitoring, Metrics, and Health Checks](#12-monitoring-metrics-and-health-checks)
13. [References & Further Reading](#13-references--further-reading)

---

## 1. High-Level System Architecture

Apache NiFi runs inside a **single Java Virtual Machine (JVM)** per node, exposing a **Jetty web server** for UI and REST access. The core runtime consists of the **Flow Controller**, a set of **Thread Pools**, and **Three Persistent Repositories**.

```
┌───────────────────────────────────────────────────────────────────────┐
│                     APACHE NIFI NODE (JVM)                            │
│                                                                       │
│  ┌───────────────────────┐   ┌───────────────────────────────────────┐ │
│  │  Jetty Web Server      │   │  REST API (Control, Metrics, Provenance)│ │
│  │  (UI, Authentication) │   └───────────────────────────────────────┘ │
│  └───────────────────────┘                                         │
│                                                                       │
│   ┌───────────────────────┐   ┌─────────────────────────────────────┐ │
│   │   Flow Controller      │   │   Repositories (Persistence Layer)   │ │
│   │   - Scheduler          │   │   • FlowFile Repository (WAL)        │ │
│   │   - Processor Graph    │   │   • Content Repository (Block Store) │ │
│   │   - Connection Queues  │   │   • Provenance Repository (Lucene)   │ │
│   └───────────────────────┘   └─────────────────────────────────────┘ │
│                                                                       │
│   ┌───────────────────────┐   ┌─────────────────────────────────────┐ │
│   │   Thread Pools         │   │   Controller Services (Shared)       │ │
│   │   • Standard (Timer)   │   │   • DB Connection Pools, SSL Context │ │
│   │   • Event-Driven       │   │   • Schema Registries, SFTP Clients  │ │
│   │   • Remote Input/Output│   └─────────────────────────────────────┘ │
│   └───────────────────────┘                                         │
└───────────────────────────────────────────────────────────────────────┘
```

Every NiFi node runs an **identical copy of the dataflow**. The cluster coordinator (selected via ZooKeeper) merely distributes *cluster-wide metadata* (e.g., variable updates) and health status; it does **not** act as a master for data processing.

---

## 2. Cluster Coordination & Zero-Master Model

NiFi’s clustering design follows the **Zero-Master** principle:
- All nodes are **peers** and can process any portion of the flow.
- A *Cluster Coordinator* (elected via ZooKeeper) is responsible for:
  - Maintaining a consistent view of the **cluster topology** (node list, status).
  - Distributing **site-to-site** flow file transfers for load balancing.
  - Propagating **global variables** and **template changes**.
- No single node is a bottleneck for data passage; each node can directly communicate with any other node using **Site-to-Site** (S2S) protocol.

```
   Node A (Coordinator)                Node B (Worker)                Node C (Worker)
   ────────────────────────   ↔   ────────────────────────   ↔   ────────────────────────
   • ZooKeeper connection            • Site-to-Site endpoint           • Site-to-Site endpoint
   • Cluster status broadcast        • Local flow execution            • Local flow execution
   • Variable sync                    (identical flow graph)           (identical flow graph)
```

**Advantages:**
- Fault tolerance: losing the Coordinator does not halt processing; a new Coordinator is elected automatically.
- Horizontal scalability: add nodes, NiFi automatically balances connections across them.

---

## 3. Core Runtime Engine: Flow Controller & Scheduling

### Flow Controller
- Parses the Directed Acycic Graph (DAG) defined by the UI/template.
- Resolves **Processor relationships** (e.g., `success`, `failure`) into **Connection** objects.
- Tracks **FlowFile state**: location, attributes, and current queue.

### Scheduling Types
| Scheduler Type | Triggering Mechanism | Typical Use Cases |
|----------------|---------------------|-------------------|
| **Timer-Driven** | Invoked on a fixed interval (`cron`, `1 sec`, `5 min`). | Periodic polling sources (SFTP, DB, HTTP). |
| **Event-Driven** | Reacts to incoming FlowFiles on a queue. | Real-time stream transforms (ConsumeKafka, ReceiveHTTP). |
| **On-Primary Node** | Executes only on the designated primary node in a cluster (useful for singleton actions). | External system registration, schema migrations. |
| **Cron Driven** | Cron expression based schedule. | Nightly batch ingestion, catalog refresh. |

The Flow Controller assigns each scheduled processor to an appropriate **Thread Pool** (see Section 5).

---

## 4. Repositories: FlowFile, Content, Provenance

| Repository | Physical Layout | Data Stored | Cleanup Strategy |
|-----------|----------------|------------|-------------------|
| **FlowFile Repository** | Write-Ahead Log (WAL) on fast SSD. Each entry is a serialized descriptor of a FlowFile (`uuid`, `attributes`, `content claim`). | In‑flight FlowFile metadata. | On graceful shutdown, entries are checkpointed; on crash, the repo is replayed to reconstruct active FlowFiles. |
| **Content Repository** | Directory of block files (`content-claim-xxxx`). Each block can be ~10 MB–1 GB depending on `nifi.content.claim.max.length`. | Binary payloads. Content is *immutable*; multiple FlowFiles can reference the same block via **reference counting**. | Async cleanup thread removes unreferenced blocks after a configurable grace period (`nifi.content.repository.cleanup.period`). |
| **Provenance Repository** | Lucene Index (or Chronicle Queue in newer versions). Indexed by UUID, timestamps, and attribute fields. | Event log (`CREATE`, `DROP`, `FORK`, `JOIN`, `SEND`, `RECEIVE`, etc.) plus a snapshot of the attributes for each event. | Retention policies (`nifi.provenance.repository.max.storage.time`, `max.storage.size`). Older events are purged automatically. |

**Key Recommendations:**
- **Store each repository on a dedicated disk** (or at least separated logical volumes). NiFi’s I/O patterns for the FlowFile Repo (random writes) differ from the Content Repo (sequential writes). Mixing them leads to high latency and backpressure.
- For high‑throughput pipelines, place the Content Repo on **NVMe SSDs** to satisfy the large sequential write/read patterns.

---

## 5. Processor Execution Model & Thread Pools

NiFi uses configurable **Thread Pools** to execute processors under different scheduling modes.

| Thread Pool | Default Size | Config Key | Typical Workload |
|-------------|--------------|------------|-----------------|
| **Standard (Timer‑Driven)** | `nifi.task.threads` (default = #CPU * 2) | `nifi.task.threads` | Polling sources, periodic transforms |
| **Event‑Driven** | Same as Standard (shared) | `nifi.event.driven.pool.size` | React to incoming FlowFiles (ConsumeKafka, ListenHTTP) |
| **Remote Input/Output** | `nifi.remote.input.socket.threads` | `nifi.remote.input.socket.threads` | Site‑to‑Site inbound/outbound connections |
| **Web UI** | `nifi.web.server.threads` | `nifi.web.server.threads` | UI rendering, REST API handling |

**Processor Parallelism:**
- Each processor instance can have a **concurrent tasks** parameter (`Run Schedule → Concurrent Tasks`). This tells the Flow Controller to schedule the same processor on multiple threads, effectively scaling horizontally within a node.
- For CPU‑intensive tasks, keep `Concurrent Tasks` ≤ **#CPU cores** to avoid context‑switch thrashing.
- For I/O‑bound processors (Kafka consumer, SFTP), you can safely set a higher concurrency (e.g., 8–16) as the threads spend most of the time blocked on network I/O.

---

## 6. Controller Services & Shared Resources

Controller Services are **singleton** objects shared across all processors that need the same resource. They are instantiated once per **Process Group** (inheritance applies to child groups).

Examples:
- **DBCPConnectionPool:** Managed JDBC connection pool; forces connection reuse and proper cleanup.
- **StandardSSLContextService:** Holds a keystore and truststore for mutual TLS; all `ListenHTTPS`, `PutSFTP` can reuse the same SSL context.
- **AvroSchemaRegistry:** Centralized schema store consulted by many `RecordReader`/`RecordWriter` services.
- **DistributedMapCacheServer / DistributedMapCacheClient:** Provides a fast, distributed key/value store for processors like `DetectDuplicate` or `UpdateAttribute`.

**Configuration Best Practices:**
- Keep the **maximum pool size** aligned with the **concurrent tasks** of processors using it.
- Enable **connection validation** (`validation-query`) for JDBC pools to evict stale connections.
- When using a **Distributed Cache**, place the cache server on a separate host (or container) to avoid saturating the NiFi node’s network interface.

---

## 7. Connections, Queues, and Prioritizers

Connections are **persistent, bounded queues** between a processor’s relationship and the next processor. They provide several key capabilities:
- **Backpressure:** Configurable thresholds based on **object count** and **size in bytes**. When the queue exceeds either threshold, upstream processors are automatically throttled.
- **Prioritization:** NiFi ships with built-in prioritizers:
  - `FirstInFirstOutPrioritizer` (default)
  - `OldestFlowFileFirstPrioritizer`
  - `PriorityAttributePrioritizer` (sorts based on a numeric attribute, e.g., `priority`)
- **Load Balancing:** For clusters, connections can be **load‑balanced** using strategies like `Round Robin`, `Single Node`, or `Partition By Attribute` across the cluster nodes.

---

## 8. Security Model: TLS, RBAC, and Encryption at Rest

### TLS & Mutual Authentication
- **Site‑to‑Site (S2S) Transport:** Uses TLS for encryption. Nodes exchange certificates signed by a central **Certificate Authority** (or self‑signed). The `Site-to-Site` protocol supports **chunked streaming** for large FlowFiles.
- **HTTPS UI:** The Jetty server can be configured with TLS (`nifi.web.https.port`, `nifi.security.keystore` settings).

### Role‑Based Access Control (RBAC)
- NiFi ships with **Apache Knox** style policies managed via the UI (`Policies` tab). Permissions are attached to **users**, **groups**, and **components** (Process Groups, Processors, Controller Services, etc.).
- Integration with external identity providers: LDAP, Kerberos, OIDC.

### Encryption at Rest
- **Content Repository Encryption:** Since version 1.13, NiFi supports **transparent encryption** of content files via the `nifi.content.repository.encryption.key.provider` configuration (e.g., `FILE`, `KMS`).
- **Provenance Repository Encryption:** Also configurable through `nifi.provenance.repository.encryption.key.provider`.
- Encrypting the **FlowFile Repository** is not recommended because it is a WAL that must be quickly writable.

---

## 9. Deployment Modes: Standalone, Clustered, and Cloud‑Native

| Mode | Description | Typical Use Cases |
|------|-------------|-------------------|
| **Standalone** | Single NiFi JVM instance, no ZooKeeper. Ideal for development, testing, or low‑volume edge ingestion. | Prototyping pipelines, single‑node edge gateway. |
| **Clustered (ZooKeeper)** | Multiple NiFi nodes share a ZooKeeper ensemble for cluster coordination. All nodes run the same flow graph. | Enterprise ingestion at scale, fault‑tolerant data logistics. |
| **Kubernetes / Docker Swarm** | NiFi container images orchestrated via Helm charts (`nifi‑helm`). StatefulSets store the repositories on PersistentVolumes. | Cloud‑native, elastic scaling, CI/CD pipeline for flow updates. |

**Configuration Highlights for Kubernetes:**
- Set `nifi.cluster.is.node=true` and `nifi.zookeeper.connect.string` to the ZK service.
- Mount PersistentVolumeClaims for `/opt/nifi/nifi-current/conf` (repo config) and `/opt/nifi/nifi-current/content_repository`, `/flowfile_repository`, `/provenance_repository`.
- Use `nifi.registry.url` for external **NiFi Registry** to manage versioned flow templates.

---

## 10. High Availability (HA) Strategies

1. **Cluster Coordination HA:** Use a **ZooKeeper ensemble** with an odd number of nodes (3,5) to avoid split‑brain.
2. **Repository Redundancy:** Deploy each repository on a **RAID‑10** or **replicated storage** (e.g., AWS EBS with volume snapshots) to survive node‑level hardware failures.
3. **Stateless Processor Design:** Keep processors **stateless** where possible; rely on external durable stores (Kafka, S3) for checkpointing.
4. **Site‑to‑Site Load Balancing:** Enable **Load Balancing Strategy = Round Robin** on connections to automatically redistribute FlowFiles if a node becomes unhealthy.
5. **Automatic FlowFile Revocation:** NiFi automatically **requeues** any FlowFile that was in‑flight on a failed node, allowing other nodes to pick it up.
6. **Backup & Restore:** Periodically snapshot the entire NiFi home directory (including `conf`, `repositories`, and the `./flow.xml.gz`). Use `nifi.sh stop && tar -czf nifi-backup.tgz $NIFI_HOME` for a quick backup.

---

## 11. Extensibility: Custom Processors & Extensions

NiFi provides a **Processor API** (Java) for building custom extensions:
- Extend `org.apache.nifi.processor.AbstractProcessor` and implement `onTrigger(ProcessContext, ProcessSession)`.
- Declare **Relationships**, **PropertyDescriptors**, and **SupportedDynamicProperties** for UI integration.
- Package the compiled JAR and drop it into `$NIFI_HOME/lib` (or a dedicated extensions directory and configure `nifi.extension.directory`).
- Optionally provide **Controller Services** for reusable resources (e.g., custom authentication providers).

**Testing:** Use the `nifi-mock` library to unit test your processor without launching a full NiFi instance.

---

## 12. Monitoring, Metrics, and Health Checks

### Built‑in Metrics
- NiFi exposes **JMX** beans (`org.apache.nifi`) for each component (Processor, Connection, JVM). Typical metrics include `FlowFilesSent`, `BytesRead`, `ProcessingTime`, `ActiveThreads`.
- The **Prometheus Reporter** (`nifi-prometheus-reporting-task`) scrapes these JMX metrics and exposes them at `/metrics`.

### Health Endpoints
- `/nifi-api/flow/status` returns overall node health (`runStatus`).
- `/nifi-api/system-diagnostics` provides CPU, memory, and disk usage.

### Log Management
- NiFi uses Logback; each node creates `nifi-app.log`, `nifi-bootstrap.log`, `nifi-user.log`. Configure rolling policies via `logback.xml`.

### Alerting
- Use **Alerting Report Tasks** (`LogAttribute`, `InvokeScriptedProcessor`) to push alerts to Slack, PagerDuty, or email on backpressure thresholds or provenance events.

---

## 13. References & Further Reading

1. **Apache NiFi Documentation – Architecture:** https://nifi.apache.org/docs/nifi-docs/html/overview.html#architecture
2. **Site‑to‑Site Protocol Specification:** https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#site-to-site-protocol
3. **NiFi Security Guide:** https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#security
4. **NiFi Clustering Best Practices:** https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#cluster-setup
5. **NiFi Record-oriented Processors:** https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#record-processing
6. **Extending NiFi – Developer Guide:** https://nifi.apache.org/docs/nifi-docs/html/developer-guide.html
7. **NiFi Helm Chart for Kubernetes:** https://github.com/helm/charts/tree/master/stable/nifi
8. **Flink‑NiFi Integration Patterns:** https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#integration-with-stream-processing-engines