# Apache Kafka: Performance Tuning & Development Best Practices

## Table of Contents
1. [Hardware & Infrastructure Planning](#1-hardware--infrastructure-planning)
2. [JVM Tuning & Memory Management](#2-jvm-tuning--memory-management)
3. [Broker Configuration & Performance Tuning](#3-broker-configuration--performance-tuning)
4. [Producer & Consumer Optimization](#4-producer--consumer-optimization)
5. [Network & Protocol Tuning](#5-network--protocol-tuning)
6. [Monitoring & Diagnostics](#6-monitoring--diagnostics)
7. [Security & Authentication](#7-security--authentication)
8. [Best Practices for Junior Data Engineers](#8-best-practices-for-junior-data-engineers)
9. [Common Anti-Patterns & Pitfalls](#9-common-anti-patterns--pitfalls)
10. [References & Further Reading](#10-references--further-reading)

---

## 1. Hardware & Infrastructure Planning

### 1.1 Storage Hierarchy

- **Broker Disks:** Use **NVMe SSDs** for optimal performance.
- **OS Page Cache:** Ensure sufficient RAM to maintain an effective OS page cache.
- **Network Storage:** For cloud-native deployments, consider using **AWS EBS** or **Azure Disk** for broker storage.

### 1.2 CPU & Memory Sizing

- **CPU:** Size brokers for **8–16 vCPUs** to allow parallelism without contention.
- **RAM:** Allocate **16–32 GB** for the JVM heap; additional OS page cache helps repository performance.
- **Network:** 10 GbE minimum for cluster nodes; 25 GbE preferred when moving >500 MB/s between nodes via Site-to-Site.

### 1.3 Container/Kubernetes Considerations

- Use **StatefulSets** with **dedicated PersistentVolumeClaims** for each broker.
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

## 3. Broker Configuration & Performance Tuning

### 3.1 Broker Configuration
- `num.network.threads` (default 3): Handle incoming network requests.
- `num.io.threads` (default 8): Handle disk I/O requests.
- `socket.receive.buffer.bytes` (default 1024k): Socket receive buffer size.
- `socket.send.buffer.bytes` (default 1024k): Socket send buffer size.

### 3.2 Performance Tuning
- `log.flush.interval.ms` (default 1000): Flush log segments every 1s.
- `log.flush.scheduler.interval.ms` (default 1000): Log flush scheduler interval.
- `log.retention.bytes` (default 1073741824): Log retention size (1 GB).
- `log.retention.hours` (default 168): Log retention time (1 week).

### 3.3 Broker Configuration for High-Throughput

- `num.partitions` (default 1): Increase for high-throughput topics.
- `num.replica.fetchers` (default 1): Increase for high-throughput topics.
- `replica.fetch.max.bytes` (default 1048576): Increase for high-throughput topics.

---

## 4. Producer & Consumer Optimization

### 4.1 Producer Optimization

- `batch.size` (default 16384): Increase for high-throughput producers.
- `linger.ms` (default 0): Increase for high-throughput producers.
- `acks` (default `all`): Set to `all` for high-throughput producers.

### 4.2 Consumer Optimization

- `fetch.min.bytes` (default 1): Increase for high-throughput consumers.
- `fetch.max.wait.ms` (default 500): Increase for high-throughput consumers.
- `max.partition.fetch.bytes` (default 1048576): Increase for high-throughput consumers.

---

## 5. Network & Protocol Tuning

### 5.1 Network Tuning

- `socket.send.buffer.bytes` (default 1024k): Increase for high-throughput networks.
- `socket.receive.buffer.bytes` (default 1024k): Increase for high-throughput networks.

### 5.2 Protocol Tuning

- `compression.type` (default `none`): Set to `gzip` or `snappy` for high-throughput networks.
- `compression.level` (default `6`): Increase for high-throughput networks.

---

## 6. Monitoring & Diagnostics

### 6.1 Monitoring

- `metrics.sample.window.ms` (default 30000): Increase for high-throughput monitoring.
- `metrics.num.samples` (default 100): Increase for high-throughput monitoring.

### 6.2 Diagnostics

- `log.level` (default `INFO`): Increase for high-throughput diagnostics.
- `log.dirs` (default `/tmp/kafka-logs`): Increase for high-throughput diagnostics.

---

## 7. Security & Authentication

### 7.1 Security

- `security.inter.broker.protocol` (default `PLAINTEXT`): Set to `SSL` or `SASL_SSL` for high-security networks.
- `ssl.keystore.location` (default `null`): Set to a secure keystore location.
- `ssl.keystore.password` (default `null`): Set to a secure keystore password.

### 7.2 Authentication

- `sasl.mechanism` (default `PLAIN`): Set to `SCRAM-SHA-256` for high-security authentication.
- `sasl.kerberos.service.name` (default `kafka`): Set to a secure Kerberos service name.

---

## 8. Best Practices for Junior Data Engineers

### 8.1 Development Workflow
1. **Design in NiFi Registry:** Version control flows via **NiFi Registry** (Git-backed). Promote flows across Dev → Test → Prod.
2. **Parameterize Everything:** Use **Parameter Contexts** for environment-specific values (hosts, ports, credentials). Never hardcode.
3. **Test Locally with MiniFi:** Use **Apache MiNiFi** (C++/Java agent) for edge testing; same processor logic, smaller footprint.
4. **Use Flow Templates:** Export/Import `.xml` templates for reusable sub-flows (e.g., "Standard Kafka Ingest", "CDC to Iceberg").

### 8.2 Building Robust Flows
- **Error Handling:** Every processor must have `failure` and `retry` relationships connected.
  - `failure` → Log + Route to Dead Letter Queue (DLQ) topic in Kafka or S3 prefix.
  - `retry` → Penalize (`PenalizeFlowFile` processor) → Loop back with exponential backoff.
- **Idempotency:** Design flows to be idempotent. Use `DetectDuplicate` with Distributed Cache for at-least-once sources.
- **Schema Contracts:** Define schemas in `AvroSchemaRegistry`; enforce with `ValidateRecord` before sink.

### 8.3 Deployment Checklist
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

## 9. Common Anti-Patterns & Pitfalls

| Anti-Pattern | Symptom | Fix |
|--------------|---------|-----|
| **No JVM heap tuning** | JVM OOM, slow checkpoints, UI lag | Set `-Xms` = `-Xmx` (no dynamic heap expansion pauses).
| **No log retention** | Disk full, NiFi crash | Set `log.retention.bytes` and `log.retention.hours`.
| **No ZooKeeper ensemble** | ZooKeeper failure, NiFi crash | Set up a 3/5 node ZooKeeper ensemble.
| **No backpressure thresholds** | Disk full, NiFi crash | Set `queue.backpressure.object.count` and `queue.backpressure.data.size`.
| **No SSL/TLS** | Data in plain text, security risk | Configure SSL/TLS for all external endpoints.
| **No RBAC policies** | Unauthorized access, security risk | Apply RBAC policies (least privilege).
| **No metrics exported** | No visibility into NiFi performance | Export metrics (Prometheus reporter enabled).
| **No alerting rules** | Silent failures, backpressure unnoticed | Set up alerting rules for backpressure, checkpoint duration, disk usage, JVM GC pause.
| **No backup strategy** | Loss of data, NiFi crash | Set up a backup strategy for `flow.xml.gz` and repo snapshots.

---

## 10. References & Further Reading

1. **NiFi System Administrator's Guide:** https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html
2. **NiFi Performance Tuning (Community Wiki):** https://cwiki.apache.org/confluence/display/NIFI/Performance+Tuning
3. **JVM Tuning for NiFi:** https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#jvm-tuning
4. **NiFi Record-oriented Processors:** https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#record-processing
5. **Extending NiFi – Developer Guide:** https://nifi.apache.org/docs/nifi-docs/html/developer-guide.html
6. **NiFi Helm Chart for Kubernetes:** https://github.com/helm/charts/tree/master/stable/nifi
7. **Flink-NiFi Integration Patterns:** https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/nifi/
8. **Streaming Systems Book:** Akidau, T., Chernyak, S., & Lax, R. (2018). *Streaming Systems.* O'Reilly Media.
9. **Kafka-NiFi Integration Patterns:** https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/
10. **Apache MiNiFi (Edge Agent):** https://nifi.apache.org/minifi/