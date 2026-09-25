---
title: "Event Time & Watermarking"
type: concept
tags:
  - concept
  - stream-processing
  - flink
  - kafka
  - time-semantics
aliases:
  - Watermarks
  - Event Time Processing
  - Watermarking
---

# ⏱️ Concept: Event Time & Watermarking

> **Definition:** In distributed streaming systems, *event time* is the timestamp embedded in the data itself (when the event actually occurred), as opposed to *processing time* (when the engine handles it). Because networks delay, reorder, and batch events, stream processors use **watermarks** — monotonically increasing progress markers that declare: *"no events with timestamp ≤ W should arrive anymore."*

## The Core Problem It Solves

Out-of-order arrival: a click event from 14:00:58 may arrive *after* one from 14:01:05 due to network partitions, producer retries, or device clock skew. Computing a per-minute window on processing time produces incorrect aggregates; event-time windows plus watermarks produce deterministic, replayable results.

## How It Works

```
Event Stream (arrival order):   e1(t=12:00:05)  e2(t=11:59:58)  e3(t=12:00:12)
                                     │               │                │
Watermark Progress:            WM = 12:00:05   (declares "all ≤ 11:59:58 done")
                                     │
                                     ▼
Window [11:59:00 - 12:00:00) fires when WM ≥ 12:00:00
```

- **Watermark generation:** Sources periodically emit `maxEventTime − allowedLateness` (e.g., a 5-second bounded out-of-orderness).
- **Window firing:** A time window closes only when the watermark crosses its end — guaranteeing late-but-within-bounds events are included.
- **Allowed lateness:** Events arriving after the watermark are routed to **side outputs** for corrective reprocessing or discarding.

## Where It Appears in This Vault

- [[Apache Flink/02_Data_Guide|Flink Data Guide]] — Flink is the canonical implementation: `EventTimeStreamAssigner`, `BoundedOutOfOrdernessWatermarks`, `allowedLateness()`, and side outputs.
- [[Apache Kafka/02_Data_Guide|Kafka Data Guide]] — Kafka Streams' `TimestampExtractor` and windowed state stores implement the same semantics; `log.append.time` vs `create.time` determines the event timestamp source.
- [[Concepts/Exactly_Once_Semantics_and_2PC|Exactly-Once Semantics]] — Deterministic event-time results are a prerequisite for replayable exactly-once recovery.

## Mental Model

Watermarking turns *"have I seen all the data?"* (unknowable) into *"I declare all data up to time W seen"* (a bounded, decidable statement). It trades **latency** (waiting for stragglers) for **correctness** (complete windows), and the `allowedLateness` knob is where you tune that trade-off.
