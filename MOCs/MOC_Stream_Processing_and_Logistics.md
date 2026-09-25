---
title: "MOC: Stream Processing & Data Logistics"
type: moc
tags:
  - moc
  - stream-processing
  - messaging
  - data-logistics
  - flink
  - kafka
  - nifi
aliases:
  - Streaming MOC
  - Ingestion MOC
---

# 🚀 MOC: Stream Processing & Data Logistics

> Map of Content for real-time streaming, continuous event processing, distributed messaging, and enterprise data logistics.

Parent: [[MOCs/00_Root_Streamhouse_MOC|Root Streamhouse MOC]]

---

## 🏗️ Core Technologies in this Domain

### 1. 🔴 [[Apache Flink/01_Overview|Apache Flink]] (Distributed Stream Processing)
- **Primary Mission:** True event-at-a-time, low-latency stateful stream computations over unbounded and bounded datasets.
- **Data Guide:** [[Apache Flink/02_Data_Guide|Time Semantics, State Backends & Network Stack]]
- **Architecture:** [[Apache Flink/03_Architecture|JobManager, TaskManager, Slots & Chandy-Lamport Snapshots]]
- **Performance:** [[Apache Flink/04_Performance_Guide|Memory Model, RocksDB Tuning & Checkpoint Optimization]]

### 2. ⚫ [[Apache Kafka/01_Overview|Apache Kafka]] (Distributed Event Backbone)
- **Primary Mission:** High-throughput, distributed, partitioned, and replicated commit log platform.
- **Data Guide:** [[Apache Kafka/02_Data_Guide|Topics, Partitions, Logs, and Consumer Groups]]
- **Architecture:** [[Apache Kafka/03_Architecture|KRaft Consensus, Broker Topology & Zero-Copy I/O]]
- **Performance:** [[Apache Kafka/04_Performance_Guide|Page Cache, Producer/Consumer Tuning & Broker Sizing]]

### 3. 🟢 [[Apache NiFi/01_Overview|Apache NiFi]] (Data Logistics & Mediation)
- **Primary Mission:** Automated, visual dataflow management, protocol translation, and end-to-end provenance.
- **Data Guide:** [[Apache NiFi/02_Data_Guide|FlowFiles, Three Repositories & Record Processing]]
- **Architecture:** [[Apache NiFi/03_Architecture|Zero-Master Clustering, Thread Pools & Controller Services]]
- **Performance:** [[Apache NiFi/04_Performance_Guide|Repository Disk Isolation, JVM Sizing & Backpressure]]

---

## 🔗 Key Conceptual Connections
- How Flink achieves exactly-once guarantees with Kafka: [[Concepts/Exactly_Once_Semantics_and_2PC|Exactly-Once & Two-Phase Commit]]
- How Flink handles out-of-order data from Kafka: [[Concepts/Event_Time_and_Watermarking|Event Time & Watermarking]]
- Real-time database sync into streaming pipelines: [[Concepts/Change_Data_Capture_CDC|Change Data Capture (CDC)]]
- Zero-copy data movement in Kafka and NiFi: [[Concepts/Zero_Copy_IO|Zero-Copy I/O]]

---

## 🗺️ Downstream Destinations
- Streaming commits into open storage: [[Apache Iceberg/01_Overview|Apache Iceberg]] · [[Apache Paimon/01_Overview|Apache Paimon]]
- Direct streaming into real-time OLAP: [[StarRocks/01_Overview|StarRocks]]
