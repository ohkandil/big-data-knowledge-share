---
title: "Apache Flink: Performance Guide"
type: guide
tags:
  - apache-flink
  - stream-processing
  - stateful-computation
  - event-time
  - watermarks
  - 04-performance-guide
aliases:
  - "Flink Performance"
  - "Flink Tuning"
  - "Flink RocksDB Tuning"
layer: "Stream Processing"
parent: "[[MOCs/MOC_Stream_Processing_and_Logistics]]"
---

# Apache Flink: Performance Tuning & Development Best Practices

## Table of Contents
1. [Memory Architecture & Optimization](#1-memory-architecture--optimization)
2. [Network Stack Tuning](#2-network-stack-tuning)
3. [Serialization & Record Layout](#3-serialization--record-layout)
4. [State Management Optimization](#4-state-management-optimization)
5. [Checkpointing Tuning](#5-checkpointing-tuning)
6. [Operator Parallelism & Scaling](#6-operator-parallelism--scaling)
7. [Time & Watermark Optimization](#7-time--watermark-optimization)
8. [Anti-Patterns & Pitfalls](#8-anti-patterns--pitfalls)
9. [Best Practices for Junior Data Engineers](#9-best-practices-for-junior-data-engineers)
10. [Monitoring & Debugging](#10-monitoring--debugging)
11. [References & Further Reading](#11-references--further-reading)

---

## 1. Memory Architecture & Optimization

Flink's memory management is designed to prevent OutOfMemoryErrors (OOM) in production while maximizing throughput. Understanding the architecture is critical for performance tuning.

### 1.1 Memory Segmentation

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         TASKMANAGER MEMORY MODEL                           │
├──────────────────────────────────────────────────────────────────────────┤
│  Process Total Memory                                                        │
│  ├─ JVM Heap Memory (Task Heap)                                              │
│  │  ├─ User Code & Objects                                                   │
│  │  ├─ Records (transient)                                                  │
│  │  └─ JVM Metaspace / Classes                                               │
│  ├─ Off-Heap Memory                                                          │
│  │  ├─ Managed Memory (Flink-controlled)                                    │
│  │  │  ├─ RocksDB State                                                    │
│  │  │  ├─ Network Buffers                                                   │
│  │  │  ├─ Sorting & Hash Tables                                             │
│  │  │  └─ Overlays (off-heap window state)                                  │
│  │  └─ Direct Memory (Netty)                                                 │
│  └─ JVM Overhead (GC, native libs, thread stacks)                            │
└──────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Key Configuration Parameters

| Parameter | Default | Purpose | Best Practice |
|---|---|---|---|
| `taskmanager.memory.process.size` | 1568 MB | Total container/process memory | Set to physical memory limit |
| `taskmanager.memory.managed.size` | Auto (1/4) | Off-heap memory for state & buffers | Increase for large state jobs |
| `taskmanager.memory.network.min/max` | Auto | Network buffer memory | Rarely need manual tuning |
| `taskmanager.memory.jvm-heap.size` | Auto | Heap for user code & transient data | Increase if frequent GC pauses |
| `taskmanager.memory.framework.heap` | Auto | Flink internal heap | Keep default unless debugging |

### 1.3 Memory Optimization Strategies

**For Large State Jobs:**
- Use `EmbeddedRocksDBStateBackend` with `state.backend.rocksdb.memory.managed: true`
- Enable incremental checkpoints: `state.backend.incremental: true`
- Set `state.backend.rocksdb.memory.high/low` watermarks to bound RocksDB memory

**For Heap-Bound Jobs (Small State):**
- Use `HashMapStateBackend` (`state.backend: hashmap`)
- Ensure `taskmanager.memory.jvm-heap.size` > estimated peak state size
- Monitor GC logs: target < 1% time in GC

**JVM GC Tuning:**
- Use G1GC for heap sizes > 4 GB
- Set `-XX:MaxGCPauseMillis=50` to keep pauses low
- Enable GC logging: `-Xlog:gc*:file=/tmp/gc.log`

---

## 2. Network Stack Tuning

The network stack moves serialized records between TaskManagers. Tuning it directly impacts throughput and latency.

### 2.1 Network Buffer Management

Flink uses direct memory buffers for network I/O. Buffer pool exhaustion causes back pressure.

| Parameter | Default | Impact |
|---|---|---|
| `taskmanager.memory.network.min/max` | 64 MB / ∞ | Total network memory |
| `taskmanager.memory.network.memory.min-segment-size` | 16 KB | Smallest buffer unit |
| `taskmanager.memory.network.memory.max-segment-size` | 64 KB | Largest buffer unit |
| `taskmanager.memory.network.memory.buffer-debloat.enabled` | false | Dynamic buffer sizing (Flink 1.14+) |

### 2.2 Buffer Debloating (Flink 1.14+)

Enable buffer debloating to dynamically adjust network buffer sizes:
```yaml
taskmanager.memory.network.memory.buffer-debloat.enabled: true
taskmanager.memory.network.memory.buffer-debloat.target: 500ms  # Target network latency
```

Benefits:
- Reduces checkpoint alignment time (fewer in-flight records)
- Improves recovery speed
- Better latency/throughput tradeoff

### 2.3 Buffer Timeout

Control the maximum time records wait in buffers before being sent:
```yaml
taskmanager.network.memory.buffer-timeout: 100ms  # Default: 100ms
```

- **Lower values** (e.g., 0ms): Minimize latency but reduce throughput
- **Higher values** (e.g., 1000ms): Maximize throughput but increase latency

### 2.4 Network Stack Optimization Checklist

- [ ] Monitor "Records Sent/Received" metrics for imbalance
- [ ] Check Web UI for back pressure levels (OK/LOW/HIGH)
- [ ] Enable buffer debloating for variable throughput jobs
- [ ] Tune buffer timeout based on latency requirements
- [ ] Ensure enough network memory to avoid buffer pool exhaustion

---

## 3. Serialization & Record Layout

Serialization is the single biggest CPU cost in distributed stream processing after business logic.

### 3.1 Type Serialization Framework

Flink uses its own type serializer framework, not Java serialization (which is slow).

```java
// POJO with public fields or getters/setters (fastest)
public class UserEvent {
    public long userId;
    public long timestamp;
    public String action;
    public double value;
}

// Avro schema (schema evolution, cross-language)
GenericRecord record = new GenericData.Record(schema);

// Kryo (fallback for complex types, slower than POJO)
env.getConfig().addDefaultKryoSerializer(MyType.class, MySerializer.class);
```

### 3.2 Performance Comparison of Serialization Strategies

| Strategy | Speed | Schema Evolution | Cross-Language | Best For |
|---|---|---|---|---|
| **Flink POJO** | Fastest | Limited | Java only | Internal types, speed critical |
| **Avro** | Fast | Excellent | Yes | Data lakes, schema evolution |
| **Protobuf** | Fast | Good | Yes | RPC, microservices integration |
| **Kryo** | Medium | Poor | Java only | Complex nested objects |
| **Java Serializable** | Slowest | Poor | Java only | Avoid in production |

### 3.3 Record Layout Best Practices

**DO:**
- Use primitives (`long`, `int`, `double`) instead of boxed types (`Long`, `Integer`, `Double`)
- Use fixed-size arrays instead of variable-size `List` where possible
- Reuse mutable objects in state updates to reduce allocation
- Implement `org.apache.flink.api.common.typeinfo.TypeInfoFactory` for custom types

**DON'T:**
- Use `java.util.Date` (use `long` epoch millis instead)
- Serialize large objects (break into smaller records)
- Use generic `Object` types (prevents Flink type analysis)

### 3.4 Serialization Tuning Example

```java
// BAD: Boxed types, no reuse
stream.map(event -> {
    String userId = event.getUserId();  // String boxing
    return new EnrichedEvent(userId, event.getValue());  // New allocation
});

// GOOD: Primitives, object reuse
stream.map(new RichMapFunction<Event, EnrichedEvent>() {
    private transient EnrichedEvent reuse;
    
    @Override
    public void open(Configuration parameters) {
        reuse = new EnrichedEvent();
    }
    
    @Override
    public EnrichedEvent map(Event event) {
        reuse.userId = event.userId;      // Primitive assignment
        reuse.value = event.value;
        return reuse;                     // Reuse same object
    }
});
```

---

## 4. State Management Optimization

State is the most critical resource in Flink. Poor state design causes checkpoint timeouts, high latency, and OOMs.

### 4.1 State Primitives Comparison

| State Type | Use Case | Performance |
|---|---|---|
| `ValueState<T>` | Single value per key | Fast (O(1) read/write) |
| `ListState<T>` | Append-only collections | Fast for appends, slow for full reads |
| `MapState<K, V>` | Key-value maps within a key | Fast for point lookups |
| `ReducingState<T>` | Incremental reduce (e.g., sum) | Very fast (never reads full state) |
| `AggregatingState<T, ACC>` | Custom aggregate functions | Fast for incremental updates |

### 4.2 State Size Optimization

**Minimize State Size:**
- Use state TTL (Time-To-Live) to expire old entries:
```java
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.hours(24))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();
```

- Use `cleanupIncrementally()` for RocksDB to reduce compaction load
- Enable state compression:
```yaml
state.backend.rocksdb.memory.managed: true
state.backend.rocksdb.predefined-options: FLASH_SSD_OPTIMIZED
```

### 4.3 RocksDB Tuning for Large State

```yaml
# Essential for > 1 GB state per slot
state.backend: rocksdb
state.backend.rocksdb.memory.managed: true
state.backend.rocksdb.memory.high: 0.7  # 70% of managed memory
state.backend.rocksdb.memory.low: 0.5   # 50% of managed memory

# Incremental checkpoints (critical for large state)
state.backend.incremental: true

# Disable WAL for pure streaming (use only if acceptable)
state.backend.rocksdb.write-batch.size: 2mb
```

### 4.4 Broadcast State Optimization

Broadcast state sends the same data to all parallel instances. Use sparingly:
- Keep broadcast state small (< 10 MB recommended)
- Use `MapState` for efficient lookups in broadcast state
- Avoid frequent updates to broadcast state (triggers serialization to all tasks)

---

## 5. Checkpointing Tuning

Checkpoint frequency and configuration directly impact recovery time and processing latency.

### 5.1 Checkpoint Interval Tradeoffs

| Interval | Use Case | Recovery Time | Overhead |
|---|---|---|---|
| 1 minute | Financial trading, critical alerting | ~1 min | High CPU/disk |
| 5 minutes | General streaming ETL | ~5 min | Moderate |
| 30 minutes | Non-critical analytics | ~30 min | Low |
| 1 hour | Batch-like streaming | ~1 hour | Minimal |

### 5.2 Checkpoint Tuning Parameters

```yaml
# Core checkpointing settings
execution.checkpointing.interval: 30s        # Balance between safety and overhead
execution.checkpointing.mode: EXACTLY_ONCE    # Use AT_LEAST_ONCE for lower latency
execution.checkpointing.timeout: 10min        # Fail job if checkpoint > 10 min
execution.checkpointing.min-pause: 30s         # Minimum time between checkpoints
execution.checkpointing.max-concurrent: 1    # Concurrent checkpoints (1 recommended)

# Externalized checkpoints (survive job cancellation)
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

# Unaligned checkpoints (Flink 1.11+) for high back pressure
execution.checkpointing.unaligned.enabled: true  # Only if network buffers are sufficient
```

### 5.3 Unaligned Checkpoints

When network buffers are full (back pressure), aligned checkpoints stall. Unaligned checkpoints snapshot in-flight data:

```yaml
execution.checkpointing.unaligned.enabled: true
execution.checkpointing.unaligned.max-subtasks-skipped: 0  # Skip alignment for all
```

**When to use:**
- High back pressure scenarios
- Small in-flight data (< 1 MB per channel)
- When checkpoint alignment takes > 1 second

**When NOT to use:**
- Low back pressure (adds unnecessary overhead)
- Very large in-flight data (increases checkpoint size)
- Jobs with AT_LEAST_ONCE sinks (alignment not required)

### 5.4 Checkpoint Storage Options

| Storage | Speed | Cost | Durability | Best For |
|---|---|---|---|---|
| JobManager Heap | Fastest | Free (memory) | Poor (process crash) | Local testing only |
| Local Filesystem | Fast | Free | Node-local | Single-node testing |
| HDFS | Medium | Medium | High | On-premise Hadoop clusters |
| S3 / GCS / ADLS | Medium | Low | Very High | Cloud-native deployments |
| RocksDB Incremental | Fast (diffs) | Low | High | Large state, cloud deployments |

---

## 6. Operator Parallelism & Scaling

Correct parallelism settings are essential for balanced workload distribution.

### 6.1 Parallelism Rules of Thumb

- **Source Parallelism:** Match Kafka topic partitions (1:1 mapping)
- **Transformation Parallelism:** Scale with CPU-bound work (8-32 typically)
- **Sink Parallelism:** Match destination throughput (e.g., Elasticsearch index shards)
- **Stateful Operators:** Prefer powers of 2 for efficient hashing (e.g., 8, 16, 32)

### 6.2 Slot Sharing Strategy

```java
// Default: all operators share slots (maximize utilization)
env.disableOperatorChaining();  // Disable only for debugging

// Isolate heavy operators to prevent slot starvation
heavyOperator.slotSharingGroup("heavy");
lightOperator.slotSharingGroup("light");
```

### 6.3 Rescaling & KeyGroup Design

Flink's key groups determine how state is partitioned. Max parallelism defines the number of key groups:

```java
// Set max parallelism at job start (cannot change later without savepoint)
env.setMaxParallelism(128);  // Must be >= max expected parallelism

// Key groups = min(parallelism, maxParallelism)
// Rescaling: state redistributed across key groups
```

**Max Parallelism Guidelines:**
- Set to expected peak parallelism (e.g., 128 for jobs that may scale to 64)
- Higher values increase metadata overhead but allow finer rescaling
- Must be set before first deployment; cannot be changed without state migration

### 6.4 Detecting & Fixing Data Skew

Data skew occurs when some keys have disproportionately many records, causing hot partitions.

**Symptoms:**
- Some subtasks process 10x more records than others
- Checkpoint timeouts on specific subtasks
- High GC pauses on specific TaskManagers

**Solutions:**
1. **Salted Keys:** Append random salt to skewed keys
```java
stream.keyBy(event -> (event.userId, Random.nextInt(10)))
      .window(...)
      .aggregate(new PartialAggregate())
      .keyBy(result -> result.userId)
      .aggregate(new FinalAggregate());
```

2. **Two-Phase Aggregation:** Local pre-aggregation before shuffle
```java
stream.keyBy(event -> event.userId)
      .window(...)  // Local window
      .aggregate(new PreAggregate())
      .keyBy(result -> result.userId)
      .window(...)  // Global window
      .aggregate(new FinalAggregate());
```

3. **Repartitioning:** Add a random shuffle to break up skew
```java
stream.rebalance()  // Random round-robin repartition
      .keyBy(event -> event.userId);
```

---

## 7. Time & Watermark Optimization

Event time processing adds complexity but ensures correctness. Optimize watermark generation for your latency requirements.

### 7.1 Watermark Generation Strategies

```java
// Strategy 1: Bounded Out-of-Orderness (most common)
WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

// Strategy 2: Monotonous (no out-of-orderness expected)
WatermarkStrategy.<Event>forMonotonousTimestamps()
    .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

// Strategy 3: Idleness (handle idle sources)
WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withIdleness(Duration.ofMinutes(5));  // Mark source idle after 5 min
```

### 7.2 Watermark Delay vs. Lateness Tradeoff

| Watermark Delay | Impact |
|---|---|
| 0 ms (processing time) | Lowest latency, incorrect for out-of-order data |
| 1-5 s | Low latency, acceptable for well-ordered sources |
| 30-60 s | Balanced for typical network jitter |
| 5-15 min | Handles extreme delays (mobile devices, batch ingestion) |

### 7.3 Window Optimization

```java
// Tumbling Window (fixed, non-overlapping)
stream.keyBy(event -> event.userId)
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .aggregate(new AverageAggregate());

// Sliding Window (overlapping, more state)
stream.keyBy(event -> event.userId)
      .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
      .aggregate(new AverageAggregate());

// Session Window (dynamic gap)
stream.keyBy(event -> event.userId)
      .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
      .allowedLateness(Time.minutes(10))
      .sideOutputLateData(lateDataTag);
```

**Window Best Practices:**
- Prefer tumbling over sliding (less state)
- Keep window size reasonable (< 1 hour for high-cardinality keys)
- Use `allowedLateness()` only when necessary (increases state)
- For session windows, ensure gap duration matches business logic

---

## 8. Anti-Patterns & Pitfalls

Common mistakes that junior engineers make when developing Flink jobs.

### 8.1 Memory-Related Anti-Patterns

| Anti-Pattern | Problem | Solution |
|---|---|---|
| **Storing large objects in state** | Causes OOM and slow checkpoints | Break into smaller records; use external storage for BLOBs |
| **Using Java serialization** | 10-100x slower than Flink serializers | Use POJOs, Avro, or Protobuf |
| **Creating objects in `map()`** | High GC pressure | Use object reuse pattern |
| **Unbounded list state growth** | OOM over time | Use state TTL or `cleanupInBackground()` |
| **Not bounding RocksDB memory** | Process killed by container orchestrator | Enable `state.backend.rocksdb.memory.managed` |

### 8.2 State Management Anti-Patterns

| Anti-Pattern | Problem | Solution |
|---|---|---|
| **Using `ValueState<List<T>>`** | Full serialization on every update | Use `ListState<T>` instead |
| **Clearing state manually** | Risk of inconsistent state | Use state TTL or timers |
| **Blocking I/O in ProcessFunction** | Back pressure propagation, checkpoint delays | Use Async I/O or side outputs |
| **Large broadcast state** | Slow checkpoint, network saturation | Keep < 10 MB, use external lookup service |

### 8.3 Checkpointing Anti-Patterns

| Anti-Pattern | Problem | Solution |
|---|---|---|
| **Checkpoint interval < 5 seconds** | Excessive CPU/disk overhead | Use 30s-5min depending on SLA |
| **No checkpoint storage configured** | State lost on job restart | Configure `state.checkpoints.dir` |
| **Incrementing parallelism without maxParallelism** | Cannot scale without savepoint | Set `maxParallelism` at deployment |
| **Ignoring checkpoint timeouts** | Silent data loss risk | Monitor checkpoint duration metric |

### 8.4 Time & Windowing Anti-Patterns

| Anti-Pattern | Problem | Solution |
|---|---|---|
| **Using processing time for financial data** | Incorrect analytics on delayed data | Always use event time with watermarks |
| **Watermarks advancing too fast** | Late data dropped silently | Use `allowedLateness()` with side outputs |
| **Session window without idleness handling** | Watermarks never advance on idle sources | Add `withIdleness()` to watermark strategy |
| **Too-small watermark delay** | Frequent late data | Match delay to source characteristics |

---

## 9. Best Practices for Junior Data Engineers

### 9.1 Development Workflow

1. **Start Local, Scale Later:**
   - Develop and test on `MiniCluster` (embedded Flink) or Docker Compose
   - Use `local` mode with parallelism 1 for debugging
   - Gradually increase parallelism to find bottlenecks

2. **Use Flink SQL First:**
   ```sql
   -- Declarative, optimized by Flink's optimizer
   CREATE TABLE kafka_source (
     user_id STRING,
     event_time TIMESTAMP(3),
     value DOUBLE,
     WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
   ) WITH ('connector' = 'kafka', ...);
   
   CREATE TABLE iceberg_sink (
     user_id STRING,
     window_start TIMESTAMP(3),
     avg_value DOUBLE
   ) WITH ('connector' = 'iceberg', ...);
   
   INSERT INTO iceberg_sink
   SELECT user_id, TUMBLE_START(event_time, INTERVAL '5' MINUTE), AVG(value)
   FROM kafka_source
   GROUP BY user_id, TUMBLE(event_time, INTERVAL '5' MINUTE);
   ```
   - Flink SQL handles serialization, state management, and windowing automatically
   - Only drop to DataStream API when SQL cannot express your logic

3. **Implement Proper Logging & Metrics:**
   ```java
   // Use Flink's metric system
   getRuntimeContext()
       .getMetricGroup()
       .counter("eventsProcessed")
       .inc();
   
   // Or use counters in ProcessFunction
   @Override
   public void processElement(Event event, Context ctx, Collector<Output> out) {
       eventCounter.inc();
       // ... processing
   }
   ```

### 9.2 Testing Strategy

```java
// Unit test with MiniCluster
@Test
public void testWindowAggregation() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    
    // Use TestStreamEnvironment for deterministic testing
    TestStreamEnvironment.setAsContext();
    
    // Add elements with event time
    DataStream<Event> stream = env.fromElements(
        new Event("user1", 1000L, 10.0),
        new Event("user1", 2000L, 20.0),
        new Event("user1", 6000L, 30.0)  // Trigger window [0, 5000)
    ).assignTimestampsAndWatermarks(...);
    
    // Execute and collect results
    List<Result> results = stream.executeAndCollect();
    assertEquals(2, results.get(0).count);  // 2 events in first window
}
```

### 9.3 Deployment Checklist

Before deploying to production:

- [ ] **Resource Allocation:**
  - [ ] Set `taskmanager.memory.process.size` based on container limits
  - [ ] Configure managed memory for state size
  - [ ] Set `taskmanager.numberOfTaskSlots` (typically 1-4 per container)

- [ ] **State Configuration:**
  - [ ] Choose state backend (RocksDB for > 1 GB state)
  - [ ] Enable incremental checkpoints
  - [ ] Set checkpoint directory (`state.checkpoints.dir`)
  - [ ] Configure state TTL where applicable

- [ ] **Reliability:**
  - [ ] Set checkpoint interval (30s-5min)
  - [ ] Configure restart strategy:
    ```yaml
    restart-strategy: fixed-delay
    restart-strategy.fixed-delay.attempts: 10
    restart-strategy.fixed-delay.delay: 10s
    ```
  - [ ] Enable exactly-once sinks (Kafka, Iceberg, JDBC with 2PC)

- [ ] **Monitoring:**
  - [ ] Expose metrics via REST API or Prometheus
  - [ ] Configure logging aggregation (ELK, Splunk)
  - [ ] Set up alerts for checkpoint failures and back pressure

---

## 10. Monitoring & Debugging

### 10.1 Key Metrics to Monitor

| Metric | Good Range | Alert Threshold |
|---|---|---|
| **Checkpoint Duration** | < interval / 2 | > checkpoint interval |
| **Checkpoint Size** | Stable or growing slowly | Sudden 10x increase |
| **Back Pressure** | OK | HIGH for > 5 minutes |
| **Records In/Out** | Steady, proportional | Drop to 0 unexpectedly |
| **GC Time Ratio** | < 1% | > 5% |
| **RocksDB Compaction Time** | < 10% of runtime | > 50% |
| **Network Buffer Utilization** | < 80% | > 95% |

### 10.2 Web UI & REST API

Access the Flink Web UI at `http://<jobmanager>:8081` for:
- **Overview:** Job DAG with throughput per operator
- **Back Pressure:** Per-subtask back pressure status
- **Checkpoints:** Duration, size, and failure history
- **Metrics:** Records in/out, watermarks, latency

Use REST API for programmatic monitoring:
```bash
# Job overview
curl http://jobmanager:8081/jobs/<job-id>

# Task metrics
curl http://taskmanager:9241/metrics?get=TaskManager.NumRecordsInPerSecond

# Checkpoint history
curl http://jobmanager:8081/jobs/<job-id>/checkpoints
```

### 10.3 Common Issues & Solutions

| Symptom | Likely Cause | Solution |
|---|---|---|
| **Checkpoint timeouts** | State too large | Enable incremental checkpoints, tune RocksDB |
| **High back pressure** | Downstream bottleneck | Scale sink, add buffering, or reduce source rate |
| **OOM killed by K8s** | Memory misconfiguration | Bound managed memory, use G1GC |
| **Late data growing** | Watermark delay too short | Increase watermark delay or use `allowedLateness()` |
| **Job restarts infinitely** | Transient sink failure | Configure restart strategy with max attempts |
| **Serialization errors** | Type mismatch | Use `TypeHint` or explicit `TypeInformation` |

---

## 11. References & Further Reading

1. **Official Flink Configuration Documentation:** [https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/config/](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/config/)
2. **State Backend Tuning:** [https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/)
3. **RocksDB Memory Tuning:** [https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/large_state_tuning/](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/large_state_tuning/)
4. **Network Buffer Debloating:** [https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/network_buffer_tuning/](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/network_buffer_tuning/)
5. **Checkpoint Tuning Guide:** [https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints/](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints/)
6. **Flink SQL Best Practices:** [https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/)
7. **Monitoring & Metrics:** [https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/)
8. **Data Skew & Optimization:** [https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/streaming_analytics/](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/streaming_analytics/)
9. **Flink Forward Talks:** [https://flink-forward.org/](https://flink-forward.org/) (search for "performance" and "tuning")
10. **Streaming Systems Book:** Akidau, T., Chernyak, S., & Lax, R. (2018). *Streaming Systems.* O'Reilly Media.


---

**Layer:** 🔴 Stream Processing  
**Parent MOC:** [[MOCs/MOC_Stream_Processing_and_Logistics|MOC: Stream Processing & Logistics]]  
**Root:** [[MOCs/00_Root_Streamhouse_MOC|Streamhouse Knowledge Map]]

**In this guide:** ← [[Apache Flink/03_Architecture|Architecture]]

**Related technologies:** [[Apache Kafka/01_Overview|Apache Kafka]] · [[Apache NiFi/01_Overview|Apache NiFi]] · [[Apache Paimon/01_Overview|Apache Paimon]] · [[Apache Iceberg/01_Overview|Apache Iceberg]]

**Core concepts:** [[Concepts/Event_Time_and_Watermarking|Event Time & Watermarking]] · [[Concepts/LSM_Trees_and_Compaction|LSM-Trees & Compaction]]
