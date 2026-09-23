# Apache NiFi: Data Guide - FlowFiles, Repositories, and Record Processing

## Table of Contents
1. [The FlowFile Abstraction](#1-the-flowfile-abstraction)
2. [Data Immutability & Content Repositories](#2-data-immutability--content-repositories)
3. [The Three-Repository Architecture](#3-the-three-repository-architecture)
4. [Record-Oriented Processing](#4-record-oriented-processing)
5. [Schema Management](#5-schema-management)
6. [Backpressure and Flow Control](#6-backpressure-and-flow-control)
7. [Data Provenance & Lineage](#7-data-provenance--lineage)
8. [Best Practices for Data Handling](#8-best-practices-for-data-handling)

---

## 1. The FlowFile Abstraction

The FlowFile is the fundamental unit of data in Apache NiFi. It does **not** hold the actual data in memory; rather, it acts as a lightweight descriptor for the data in transit.

A FlowFile is composed of two discrete components:
- **Attributes:** Key-value string pairs (e.g., `mime.type`, `uuid`, `filename`, `user.id`). These are **always** stored in the JVM heap memory for quick access. They are used for routing, enrichment, and filtering decisions.
- **Content Pointer (Claim):** A handle to the actual payload bytes stored in the Content Repository on disk.

When a processor "processes" a FlowFile, it reads the attributes from memory and streams the content from disk as needed.

---

## 2. Data Immutability & Content Repositories

NiFi treats content as **immutable**.

- **Copy-on-Write/Append:** If an operation modifies a FlowFile's content (e.g., `ReplaceText`), NiFi **never** updates the original bytes in place. Instead, it writes the modified content to a new block in the Content Repository, and the FlowFile is updated to point to this new "content claim."
- **Reference Counting:** The underlying content block remains in the repository as long as at least one FlowFile (or Provenance Event) references it.
- **Asynchronous Cleanup:** Once a FlowFile is dropped or terminated, the reference count drops, and NiFi’s asynchronous cleanup thread (the `ContentRepository` maintenance background task) physically deletes the unreferenced content blocks from the disk.

This immutability is essential for **fault tolerance and lineage**. If a pipeline step downstream fails, the upstream processor still has a valid content claim to re-try or route to an error path.

---

## 3. The Three-Repository Architecture

These three primary storage segments drive all NiFi operations:

| Repository | Purpose | Storage Type | Tuning Focus |
| ----------- | ----------- | ----------- | ----------- |
| **FlowFile Repo** | Stores metadata (attributes, status) | Write-Ahead Log (WAL) | Faster disk (SSD), I/O ops |
| **Content Repo** | Stores actual payload bytes | Block Storage (Claims) | High throughput, Large capacity |
| **Provenance Repo**| Stores event history/lineage | Lucene Index / Chronicle | High volume, high throughput |

**Crucial Architecture Requirement:** Each of these repositories **must** be mapped to a physically distinct storage volume (e.g., separate NVMe drives/partitions). Contention between these on the same physical disk is the #1 cause of NiFi performance degradation.

---

## 4. Record-Oriented Processing

Traditional NiFi dataflows often used a "Split -> Transform -> Merge" pattern (e.g., split a 10MB JSON file into 10,000 FlowFiles, transform each, then merge). This is extremely inefficient because:
1. Each small FlowFile generates multiple Provenance Events.
2. Each small FlowFile incurs FlowFile Repository I/O overhead.
3. Heap usage explodes to manage metadata for thousands of FlowFiles.

**Record-Oriented Processing** (via `RecordReader` and `RecordWriter` Controller Services) solves this by streaming throughput:
- The input is treated as a stream of records (rows).
- The processor reads the entire file as a single FlowFile.
- Transformations (e.g., `QueryRecord`, `UpdateRecord`) are applied **in-stream** to the records.
- The output is written as a single FlowFile, preserving the original structure or format (Avro, JSON, CSV).

**Benefit:** Drastic reduction in disk I/O, Provenance noise, and JVM memory pressure.

---

## 5. Schema Management

Record processors require a schema to know how to map data structures. NiFi manages this via the **Schema Registry Controller Service**.

- **AvroSchemaRegistry:** Defines schemas using Avro IDL (JSON).
- **Processors use schema references:** You configure a `RecordReader` (e.g., `JsonTreeReader`) to look up the schema by name or ID in the `AvroSchemaRegistry`.
- **Schema Evolution:** If an incoming JSON field is added, the schema registry handles the evolution rule (e.g., `default` values in Avro), ensuring downstream `ConvertRecord` processors do not break.

---

## 6. Backpressure and Flow Control

Backpressure ensures downstream system safety by creating a physical feedback loop:
- Thresholds are defined on a per-Connection (queue) basis:
  - **Backpressure Object Threshold:** e.g., Max 10,000 FlowFiles.
  - **Backpressure Data Size Threshold:** e.g., Max 1 GB.
- When an object or size count hits the threshold, the upstream processor's **scheduling is effectively paused** by the core engine.
- Data stops flowing from the source until the downstream processor drains the queue below the 70% threshold.

---

## 7. Data Provenance & Lineage

The Provenance Repository is the core of NiFi's governance capabilities.

- **Storage:** Uses a time-series indexed structure (Lucene or Chronical Queue).
- **Lineage:** Links FlowFile events chronologically. You can search by `filename`, `uuid`, or attribute values to find where a record originated, how it was filtered, and where it was sent.
- **Replay:** Because content is stored immutably, you can command a processor to re-process an event from weeks ago by re-injecting the content claim from the archive.

---

## 8. Best Practices for Data Handling

1. **Isolation is King:** Always dedicate separate physical drives (ideally NVMe SSDs) for Content, FlowFile, and Provenance repos.
2. **Prioritize Records:** Almost always choose `Record`-based equivalents (e.g., `ConvertRecord` over `ValidateXml`, `UpdateRecord` over `ReplaceText`).
3. **Control Provenance Size:** For massive throughput, tune `nifi.provenance.repository.max.storage.time` and `nifi.provenance.repository.max.storage.size` to prevent repository growth from filling mount points.
4. **Avoid Heap Bloat:** Never store massive binary payloads as attributes. Attributes are in-memory. If a payload is >1MB, ensure it is treated as content, not an attribute.
5. **Backpressure Sizing:** Size backpressure thresholds based on the time required to recover from a downstream system outage. If a target is down for 30 minutes, ensure the queue threshold holds at least 30 minutes worth of data.
