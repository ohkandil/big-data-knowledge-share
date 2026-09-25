---
title: "Apache Kafka: Architecture"
type: architecture
tags:
  - apache-kafka
  - messaging
  - event-streaming
  - distributed-log
  - kraft
  - 03-architecture
aliases:
  - "Kafka Architecture"
  - "KRaft"
  - "Kafka Brokers"
  - "Kafka Zero-Copy"
layer: "Distributed Event Log"
parent: "[[MOCs/MOC_Stream_Processing_and_Logistics]]"
---

# Apache Kafka: Architecture & Deployment Topology

## Table of Contents
1. [High-Level System Architecture](#1-high-level-system-architecture)
2. [Broker Architecture and Responsibilities](#2-broker-architecture-and-responsibilities)
3. [KRaft Consensus and Controller Nodes](#3-kraft-consensus-and-controller-nodes)
4. [Partition and Replica Management](#4-partition-and-replica-management)
5. [Storage Architecture and Log Segments](#5-storage-architecture-and-log-segments)
6. [Network Architecture and Zero-Copy I/O](#6-network-architecture-and-zero-copy-io)
7. [Deployment Modes and Cloud Native considerations](#7-deployment-modes-and-cloud-native-considerations)
8. [High Availability (HA) Strategies](#8-high-availability-ha-strategies)
9. [References & Further Reading](#9-references--further-reading)

---

## 1. High-Level System Architecture

Apache Kafka operates as a distributed cluster of broker nodes. The architecture is designed for extreme throughput, horizontal scalability, and high durability through data replication.

Kafka’s control plane handles metadata, leader election, and cluster membership, while the data plane handles the actual streaming of bytes to and from persistent storage.

```
┌────────────────────────────────────────────────────────────────────────┐
│                      APACHE KAFKA CLUSTER (KRaft Mode)                 │
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                     CONTROLLER QUORUM (Raft)                     │  │
│  │                                                                  │  │
│  │   [ Controller 1 ] ◄────► [ Controller 2 ] ◄────► [ Controller 3 │  │
│  │                              (Active Leader)                     │  │
│  │                                                                  │  │
│  │   Topic / Partition Metadata, Broker Membership, Access Control  │  │
│  └─────────────────────────────────┬────────────────────────────────┘  │
│                                    │ Metadata Sync via @metadata topic │
│                                    ▼                                   │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                            BROKER POOL                           │  │
│  │                                                                  │  │
│  │  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────┐ │  │
│  │  │ Broker 1          │  │ Broker 2          │  │ Broker 3      │ │  │
│  │  │ - P0 (Leader)     │◄─┤ - P0 (Follower)   │◄─┤ - P2 (Leader) │ │  │
│  │  │ - P1 (Follower)   │  │ - P1 (Leader)     │  │ - P1 (Follow) │ │  │
│  │  │ - Log Segments    │  │ - Log Segments    │  │ - Log Segments│ │  │
│  │  │ - Page Cache      │  │ - Page Cache      │  │ - Page Cache  │ │  │
│  │  └────────┬──────────┘  └────────┬──────────┘  └───────┬───────┘ │  │
│  └───────────┼──────────────────────┼─────────────────────┼─────────┘  │
└──────────────┼──────────────────────┼─────────────────────┼────────────┘
               │                      │                     │
               ▼                      ▼                     ▼
     [ PRODUCER CLIENTS ]                    [ CONSUMER CLIENTS ]
     (Send to partition leaders)             (Fetch from partition leaders)
```

---

## 2. Broker Architecture and Responsibilities

A Kafka Broker is a single JVM process running on a server. Its primary job is to receive messages from producers, store them on disk, and serve them to consumers.

### Internal Broker Components:
1. **Network Threads (`num.network.threads`):** Handle incoming network requests from clients and other brokers. They parse the raw bytes into Kafka protocol requests.
2. **I/O Threads (`num.io.threads`):** The network threads hand off validated requests to a request queue. I/O threads pick up these requests and perform the actual disk reads/writes.
3. **Log Manager:** Manages the logical partitions and their physical mapping to directories and segment files on local storage.
4. **Replica Fetcher Threads:** If this broker is a follower for a partition, these threads actively pull data from the leader broker to keep the local replica in sync.
5. **Purgatory:** An internal data structure (Timing Wheel) holding delayed requests, such as producer requests waiting for `acks=all` from followers, or long-polling consumer fetch requests.

---

## 3. KRaft Consensus and Controller Nodes

Since version 3.3, Kafka has removed its dependency on ZooKeeper (Project KRaft - KIP-500).

### The Role of the Controller
The cluster requires an active Controller to manage state. The Controller:
- Detects broker failures and triggers leader elections for affected partitions.
- Manages topic creation, deletion, and partition expansion.
- Maintains cluster membership and quotas.

### How KRaft Works
- Instead of using ZooKeeper, KRaft implements an event-driven Raft consensus algorithm inside Kafka.
- Metadata is stored as a single, internal Kafka topic named `@metadata`.
- A configured subset of brokers (or dedicated nodes) acting as **Controllers** form a Raft Quorum.
- The active Controller appends metadata changes (e.g., "Broker 3 died, Partition 1 leader is now Broker 2") to the `@metadata` log.
- All other brokers consume this `@metadata` log to maintain a real-time, synchronized view of the cluster state in their local memory.

**Advantage:** Sub-second leader election during failures, allowing Kafka to host millions of partitions per cluster (a scale impossibility with legacy ZooKeeper).

---

## 4. Partition and Replica Management

### The Partition as the Unit of Scale
A Topic in Kafka is conceptually a single log, but physical limitations mean one broker cannot handle unlimited throughput. Therefore, topics are divided into **Partitions**.

- **Producers** dictate partitioning: They apply a hashing function (like Murmur2) to the message Key to route traffic (e.g., `hash(user_id) % num_partitions`). If no key is provided, records are sent round-robin or via sticky partitioning to balance load.
- **Consumers** scale by partition: A single partition can only be read by one consumer instance within a consumer group. To increase consumer throughput, you must increase the partition count.

### Replication and High Availability
Partitions are replicated across multiple brokers (typically a Replication Factor of 3).
- **Leader:** One replica is elected Leader. All producer writes and consumer reads (traditionally) go through the Leader.
- **Follower:** Other replicas passively fetch records from the Leader.
- **In-Sync Replicas (ISR):** A subset of replicas that are fully caught up with the Leader (lag time `< replica.lag.time.max.ms`).
- If the Leader fails, the Controller instantly promotes one of the ISRs to be the new Leader.

---

## 5. Storage Architecture and Log Segments

Kafka does not store data in a complex database engine; it writes pure formatted bytes sequentially to disk.

### Physical Mapping
A partition maps to a physical directory on a broker's disk (e.g., `/var/lib/kafka/data/orders-topic-0/`).

Inside this directory, the partition log is split into **Segments**:
1. **`.log` file:** The actual record payloads appended sequentially.
2. **`.index` file:** Maps logical offsets to physical byte positions in the `.log` file (enables fast `seek()` operations).
3. **`.timeindex` file:** Maps timestamps to offsets (enables consumers to search/read from a specific point in time).

### Segment Rolling and Retention
Kafka appends to the *active* segment until it reaches `segment.bytes` (default 1GB) or `segment.ms` (default 7 days). The segment is then "rolled" (closed), and a new active segment is created.
Closed segments are evaluated against retention policies (`log.retention.hours` or `log.retention.bytes`) and deleted completely from disk when they expire.

---

## 6. Network Architecture and Zero-Copy I/O

Kafka's massive throughput capability stems directly from how it interacts with the OS network and storage layers.

### Sequential I/O and OS Page Cache
Kafka avoids managing its own in-heap memory cache. Instead, it relies on the OS Kernel's **Page Cache**.
- When a producer writes records, Kafka appends them to the filesystem, and Linux caches them in available RAM.
- When a consumer reads records, Kafka fetches them directly from this OS Page Cache, rarely touching physical disk blocks.
- This bypasses the JVM entirely, avoiding Garbage Collection overhead.

### Zero-Copy (The `sendfile` syscall)
When a consumer requests data (and it is stored on disk instead of the Page Cache), Kafka uses Zero-Copy.
- **Without Zero-Copy:** Kernel reads disk → Memory → JVM Heap → Socket Buffer (Kernel) → NIC. (4 context switches, high CPU cost).
- **With Zero-Copy:** Kernel initiates DMA (Direct Memory Access). Bytes flow directly from the Disk/Page Cache to the Network Interface Card (NIC) buffer. (Bypasses user-space/JVM entirely, near zero CPU overhead).

---

## 7. Deployment Modes and Cloud Native considerations

- **Bare Metal / VMs:** The traditional and most performant deployment. Kafka relies heavily on local NVMe/SSD disks and vast OS RAM for page caching.
- **Kubernetes:** Usually orchestrated via operators like Strimzi or Confluent for Kubernetes. Brokers map to StatefulSets with PersistentVolumes.
- **Multi-AZ / Rack Awareness:** Brokers can be tagged with `broker.rack` attributes (e.g., mapping to AWS AZs). Kafka's replica placement algorithm ensures that a partition's replicas are distributed across different racks/AZs, surviving a complete datacenter loss.
- **Tiered Storage (KIP-405):** A modern architectural shift where old, closed log segments are moved off local broker SSDs onto cheap object storage (S3). This turns Kafka into an infinite-retention data lake without blowing up local hardware costs, decoupling compute (brokers) from cold storage.

---

## 8. High Availability (HA) Strategies

1. **Replication Factor = 3:** Guarantees data survival.
2. **`min.insync.replicas` = 2:** Require at least 2 brokers to acknowledge a write before telling the producer it succeeded.
3. **Producer `acks=all`:** Forces the leader to wait for the followers (meeting the ISR minimum) to write the data.
4. **Rack Awareness:** Prevents all 3 replicas from living in the same isolated Rack or Availability Zone.
5. **Unclean Leader Election = `false`:** Prevents an out-of-sync replica from becoming a leader (which would cause data loss via truncation). It prioritizes consistency over availability.

---

## 9. References & Further Reading

1. **Apache Kafka KRaft Architecture:** https://developer.confluent.io/learn/kraft/
2. **Kafka Tiered Storage:** https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage
3. **Kafka Zero-Copy Mechanics:** https://developer.ibm.com/articles/j-zerocopy/
4. **Strimzi (Kafka on K8s):** https://strimzi.io/


---

**Layer:** ⚫ Distributed Event Log  
**Parent MOC:** [[MOCs/MOC_Stream_Processing_and_Logistics|MOC: Stream Processing & Logistics]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** ← [[Apache Kafka/02_Data_Guide|Data Guide]]  |  → [[Apache Kafka/04_Performance_Guide|Performance Guide]]

**Related technologies:** [[Apache Flink/01_Overview|Apache Flink]] · [[Apache NiFi/01_Overview|Apache NiFi]] · [[Apache Paimon/01_Overview|Apache Paimon]] · [[Apache Iceberg/01_Overview|Apache Iceberg]]

**Core concepts:** [[Concepts/Event_Time_and_Watermarking|Event Time & Watermarking]]
