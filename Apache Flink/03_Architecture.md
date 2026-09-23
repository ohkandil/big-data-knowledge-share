# Apache Flink: Architecture & Deployment Topology

## Table of Contents
1. [High-Level System Architecture](#1-high-level-system-architecture)
2. [JobManager: The Master Node](#2-jobmanager-the-master-node)
3. [TaskManager: The Worker Node](#3-taskmanager-the-worker-node)
4. [Resource Management & Task Slots](#4-resource-management--task-slots)
5. [Communication Layers: RPC & Netty](#5-communication-layers-rpc--netty)
6. [Deployment Modes & Cluster Managers](#6-deployment-modes--cluster-managers)
7. [Job Lifecycle: Submission to Execution](#7-job-lifecycle-submission-to-execution)
8. [Fault Tolerance & Recovery Mechanisms](#8-fault-tolerance--recovery-mechanisms)
9. [Parallelism, Operator Chaining & Task Execution](#9-parallelism--operator-chaining--task-execution)
10. [References & Further Reading](#10-references--further-reading)

---

## 1. High-Level System Architecture

Apache Flink follows a **distributed master-worker architecture** with clear separation of concerns between the control plane (JobManager) and data plane (TaskManagers). The system is designed for elasticity, fault tolerance, and high-throughput stream processing.

```
                            ┌─────────────────────────────────────┐
                            │            CLIENT / CLI             │
                            │ (Compiles job to JobGraph)          │
                            └───────────────────────┬─────────────┘
                                                    │ JobGraph Submission
                                                    ▼
    ┌─────────────────────────────────────────────────────────────────────────────┐
    │                                   JOBMANAGER                                │
    │  ┌──────────────────┐  ┌────────────────────┐  ┌───────────────────────┐  │
    │  │   Dispatcher     │  │ ResourceManager    │  │       JobMaster       │  │
    │  │ (REST, WebUI,    │  │ (Slot allocation,  │  │ (Task scheduling,     │  │
    │  │ Job submission)  │  │  slot management)  │  │  checkpoint coord.)   │  │
    │  └──────────────────┘  └────────────────────┘  └───────────────────────┘  │
    │  ┌───────────────────────────────────────────────────────────────────────┐  │
    │  │ Checkpoint Coordinator (Triggers & manages distributed snapshots)    │  │
    │  └───────────────────────────────────────────────────────────────────────┘  │
    └───────────────────────┬─────────────────────────────┬─────────────────────┘
                            │ Heartbeat / RPC             │ Heartbeat / RPC
                            ▼                             ▼
    ┌───────────────────────────────────────┐ ┌───────────────────────────────────────┐
    │            TASKMANAGER 1              │ │            TASKMANAGER 2              │
    │  ┌──────────────┐ ┌────────────────┐  │ │  ┌──────────────┐ ┌────────────────┐  │
    │  │   Slot 1     │ │   Slot 2       │  │ │  │   Slot 1     │ │   Slot 2       │  │
    │  │ ┌──────────┐ │ │ ┌──────────┐ │  │ │  │ ┌──────────┐ │ │ ┌──────────┐ │  │
    │  │ │ Operator │ │ │ │ Operator │ │  │ │  │ │ Operator │ │ │ │ Operator │ │  │
    │  │ │ (Source) │ │ │ │ (Map)    │ │  │ │  │ │ (Filter) │ │ │ │ (Window) │ │  │
    │  │ └──────────┘ │ │ └──────────┘ │  │ │  │ └──────────┘ │ │ └──────────┘ │  │
    │  └──────────────┘ └────────────────┘  │ │  └──────────────┘ └────────────────┘  │
    │  ┌─────────────────────────────────┐  │ │  ┌─────────────────────────────────┐  │
    │  │ Netty Transport & NetworkBuffer │◄─┼─┼─►│ Netty Transport & NetworkBuffer │  │
    │  │ Managed State (RocksDB/Heap)    │  │ │  │ Managed State (RocksDB/Heap)    │  │
    │  └─────────────────────────────────┘  │ │  └─────────────────────────────────┘  │
    └───────────────────────────────────────┘ └───────────────────────────────────────┘
```

### Key Design Principles:
- **Separation of Concerns:** JobManager (control) never sees data records; TaskManagers (work) handle all data movement.
- **Elastic Resource Allocation:** Task slots are dynamically allocated based on available resources and job parallelism.
- **Zero-Copy Data Transfer:** Uses Netty direct buffers and memory pooling to minimize serialization and network overhead.
- **Fault-Tolerant by Design:** Checkpointing and recovery mechanisms are built into the core architecture.

---

## 2. JobManager: The Master Node

The JobManager (sometimes called the master) is responsible for coordinating the distributed execution of a Flink application. It does **not** process any data records itself.

### Components:
1. **Dispatcher:**
   - Exposes REST API and Web UI for job submission, monitoring, and cancellation.
   - Maintains the cluster state and available job jars.
   - Can run in high-availability mode (multiple Dispatchers with leader election via ZooKeeper).

2. **ResourceManager:**
   - Manages the pool of TaskManager slots across the cluster.
   - Responds to slot requests from the JobMaster and offers available slots.
   - Implements different resource managers for YARN, Kubernetes, and standalone modes.

3. **JobMaster (per-job master):**
   - Created for each submitted job (one JobMaster per job).
   - Responsibilities:
     - Schedules tasks (operators) to available TaskManager slots.
     - Monitors task execution (heartbeats, timeouts).
     - Triggers and coordinates distributed checkpoints.
     - Handles failure recovery (restarting failed tasks from latest checkpoint).
   - Contains the **Scheduler** that implements slot sharing and slot-independent scheduling.

4. **Checkpoint Coordinator:**
   - Part of the JobMaster; responsible for initiating checkpoint barriers.
   - Coordinates the asynchronous barrier snapshotting algorithm (variant of Chandy-Lamport).
   - Manages checkpoint metadata and retention policies.

### Communication:
- Uses **Akka/Akka Pekko** for RPC between JobManager components and with TaskManagers.
- Heartbeats sent every 10 seconds (configurable) to detect failures.
- Job submission: Client uploads JAR to Dispatcher, which stores it and notifies JobMaster.

---

## 3. TaskManager: The Worker Node

TaskManagers are the worker processes that execute the actual data processing logic. Each TaskManager provides one or more **task slots** for executing parallel sub tasks.

### Components per TaskManager:
1. **Task Slots:**
   - The unit of resource isolation in Flink.
   - Each slot represents a fixed subset of the TaskManager's resources (CPU, memory, network).
   - Slots enable **slot sharing**: multiple operators from the same job can share a slot to reduce startup overhead and improve resource utilization.

2. **Memory Management:**
   - Divides memory into distinct regions:
     - **Managed Memory:** Used for state (RocksDB, hash tables), network buffers, and sorting.
     - **Heap Memory:** For user code, deserialization objects, and transient data.
     - **Off-Heap Memory:** Direct buffers for network transfer and RocksDB storage.
   - Total process memory is bounded to prevent OOM kills in containerized environments.

3. **Network Stack:**
   - Built on **Netty** for high-performance, asynchronous I/O.
   - Uses **network buffers** (direct memory) for zero-copy data transfer between TaskManagers.
   - Implements credit-based flow control to prevent buffer overflow.

4. **Execution Environment:**
   - Each slot runs tasks in its own thread (or shares threads via slot sharing).
   - Provides the runtime environment for operators (serialization, metrics, logging).

### Task Slot Sharing:
- **Why Share Slots?** Reduces the number of required TaskManagers, decreases startup overhead, and improves resource utilization for pipelined streams.
- **How It Works:** Multiple operators (e.g., Source → Map → Filter) can be chained and executed sequentially within the same thread/slot.
- **Limitations:** Only operators with compatible chaining rules (same key grouping, no rebalancing) can share a slot.

---

## 4. Resource Management & Task Slots

Flink's resource model centers around the concept of **task slots**, which define the granularity of resource allocation and isolation.

### Slot Composition:
A task slot is a proportional fraction of a TaskManager's resources:
- **CPU:** Typically one core per slot (but can be oversubscribed).
- **Memory:** Managed memory heap divided equally among slots.
- **Network:** Each slot gets a portion of the network buffers and bandwidth.

### Resource Allocation Flow:
1. **Job Submission:** Client submits job with specified parallelism (e.g., `parallelism: 4`).
2. **ResourceManager:** JobMaster requests N slots from ResourceManager.
3. **Slot Offering:** TaskManagers report available slots to ResourceManager.
4. **Slot Allocation:** ResourceManager grants slots to JobMaster.
5. **Task Deployment:** JobMaster deploys tasks to allocated slots in TaskManagers.

### Slot Sharing Groups:
- By default, all operators in a job belong to the same slot sharing group.
- Operators can be isolated into different groups to prevent chaining (e.g., for CPU-intensive vs. I/O-bound operators).
- Defined via `slotSharingGroup("name")` in the DataStream API.

### Memory Configuration:
- `taskmanager.memory.process.size`: Total process memory (default: 1568 MB).
- `taskmanager.memory.managed.size`: Portion for state (default: 1/4 of process memory).
- `taskmanager.memory.network.min/max`: Network buffer memory (auto-tuned by default).
- `taskmanager.memory.heap.size`: Remaining memory for JVM heap (user code, deserialization).

---

## 5. Communication Layers: RPC & Netty

Flink uses a layered communication approach: high-level coordination via RPC and high-throughput data transfer via Netty.

### 5.1 RPC Layer (Akka/Akka Pekko)
- **Purpose:** Control-plane communication (job management, heartbeats, checkpoint coordination).
- **Characteristics:**
  - Request/response pattern for infrequent, low-volume messages.
  - Built-in fault detection via death watch and heartbeats.
  - Supports Akka clustering for high availability (multiple JobManagers).
- **Typical Messages:**
  - Job submission, job cancellation
  - Task deployment, task completion
  - Checkpoint barrier alignment, checkpoint completion
  - Heartbeats (every 10s by default)

### 5.2 Data Plane Layer (Netty)
- **Purpose:** High-volume, low-latency transfer of data records between TaskManagers.
- **Characteristics:**
  - Fully asynchronous, non-blocking I/O.
  - Uses direct memory buffers (ByteBuffer) for zero-copy transfers.
  - Implements credit-based flow control to prevent receiver overload.
  - Supports both pipelined (streaming) and blocking (batch-style) data exchange.
- **Network Stack Components:**
  - **NetworkBufferPool:** Pre-allocated pool of direct buffers for reuse.
  - **Credit-based Flow Control:** Sender waits for receiver credits before sending.
  - **ResultPartitions:** Logical partitions of output data (pipelined or blocking).
  - **Gateways:** Abstract over network communication for local vs. remote transfer.

### 5.3 Data Exchange Patterns:
- **Local Transfer:** Within the same TaskManager (same slot or different slots) uses Java object references or direct memory copy.
- **Remote Transfer:** Between TaskManagers uses Netty with serialization (via Flink's TypeSerializer framework).
- **Serialization:** Flink uses custom serializers (avro, kryo, or user-defined) to convert objects to bytes for network transfer.

---

## 6. Deployment Modes & Cluster Managers

Flink is designed to run on various resource managers and can also operate in standalone mode.

### 6.1 Standalone Cluster
- **Description:** Manual deployment of JobManager and TaskManager processes on static machines.
- **Use Cases:** Development, testing, small production clusters.
- **Pros:** Simple to set up and manage.
- **Cons:** Manual scaling, no automatic fault tolerance for node loss.

### 6.2 Hadoop YARN
- **Description:** Flink runs as an application on YARN, leveraging YARN's resource management.
- **Use Cases:** Existing Hadoop environments, multi-tenant clusters sharing resources with MapReduce/Spark.
- **How It Works:**
  1. Flink JobManager requests containers from YARN ResourceManager.
  2. YARN launches TaskManager containers on NodeManagers.
  3. Flink monitors container health and requests new containers on failure.

### 6.3 Kubernetes
- **Description:** Flink runs as a set of Kubernetes pods managed by the Flink Kubernetes Operator.
- **Use Cases:** Cloud-native deployments, CI/CD integration, autoscaling.
- **Components:**
  - **JobManager Deployment:** Runs as a pod (can be highly available with multiple replicas).
  - **TaskManager Deployment:** Scales via ReplicaSet or StatefulSet.
  - **Flink Kubernetes Operator:** Manages FlinkApplication CRDs, handles upgrades and scaling.

### 6.4 Native Kubernetes (Session Mode vs. Application Mode)
- **Session Mode:** Long-running Flink cluster where multiple jobs share the same JobManager/TaskManagers.
- **Application Mode:** Dedicated cluster per job (JobManager and TaskManagers deployed for that job only).

### 6.5 Cloud Providers (AWS EMR, GCP Dataproc, Azure HDInsight)
- **Description:** Flink is offered as a managed service or via quick-start templates.
- **Use Cases:** Fully managed Flink with integrated monitoring and scaling.

### 6.6 Resource Isolation & Multi-tenancy
- **Slot Isolation:** Tasks from different jobs can run in the same slot if slot sharing is allowed.
- **Job Isolation:** Use separate Flink clusters (different JobManagers) for strong isolation.
- **Resource Quotas:** Set via YARN/Kubernetes (CPU/memory limits per namespace/job).

---

## 7. Job Lifecycle: Submission to Execution

Understanding the job lifecycle helps in debugging, monitoring, and optimizing Flink applications.

### 7.1 Job Submission
1. **Client Action:** User runs `flink run -d job.jar` or submits via REST API.
2. **Dispatch Phase:**
   - Client uploads JAR to Dispatcher (via REST or directly).
   - Dispatcher stores JAR and returns a job ID.
   - Dispatcher notifies the leading JobMaster of new job submission.
3. **JobGraph Construction:**
   - JobMaster loads the JAR and constructs the initial StreamGraph from user code.
   - StreamGraph is optimized (operator chaining, scaling) into a JobGraph.
   - JobGraph includes parallelism, chaining hints, and operator metadata.

### 7.2 Job Initialization
1. **Resource Allocation:**
   - JobMaster requests required slots from ResourceManager based on JobGraph parallelism.
   - ResourceManager allocates slots from available TaskManagers.
2. **Task Deployment:**
   - JobMaster deploys task execution graphs to TaskManagers.
   - Each task receives its configuration (parallelism, index, operator code).
   - TaskManagers instantiate the operator chain in the allocated slot.

### 7.3 Job Execution
1. **Task Startup:**
   - Tasks initialize operators (open() method), establish network connections.
   - Sources begin emitting data (e.g., Kafka consumer starts reading).
2. **Steady-State Processing:**
   - Data flows through operator chain: source → transform → sink.
   - Network buffers exchange records between TaskManagers.
   - State is updated locally; checkpoints are triggered periodically.
3. **Monitoring & Metrics:**
   - JobMaster collects metrics from tasks (via REST or gossip).
   - Web UI displays throughput, latency, checkpoint status, and back pressure.

### 7.4 Job Termination
1. **Normal Completion:**
   - Sources emit end-of-stream (for bounded data).
   - Tasks finish processing, send final checkpoint, and shut down.
2. **Cancellation:**
   - User requests job cancel via CLI/Web UI.
   - JobMaster stops sending new data, waits for in-flight checkpoints, then shuts down tasks.
3. **Failure Recovery:**
   - TaskManager failure detected via missed heartbeats.
   - JobMaster tasks are restarted from latest successful checkpoint.
   - If checkpointing fails repeatedly, job may fail permanently.

### 7.5 Savepoints & Upgrades
1. **Savepoint Trigger:** User triggers savepoint via CLI/Web UI.
2. **Job Quiescence:** JobMaster initiates checkpoint, waits for completion.
3. **State Export:** Checkpoint metadata saved to external location (savepoint).
4. **Job Restart:** New job started from savepoint (allows code changes, Flink version upgrade).

---

## 8. Fault Tolerance & Recovery Mechanisms

Flink's fault tolerance is achieved through **distributed snapshotting** (checkpointing) and **redo-style recovery**.

### 8.1 Checkpointing Mechanism
Flink uses a variant of the **Chandy-Lamport algorithm** called **asynchronous barrier snapshotting**.

#### How It Works:
1. **Checkpoint Barrier Injection:**
   - Checkpoint Coordinator (JobMaster) broadcasts a barrier to all source operators.
   - Sources emit a barrier record after emitting all current data records.
2. **Barrier Alignment:**
   - Operators forward barriers downstream upon receipt.
   - When an operator has received a barrier from all input channels, it:
     - Takes a snapshot of its state (to managed memory or RocksDB).
     - Forwards the barrier downstream.
3. **Sink Acknowledgement:**
   - Sink operators receive barriers from all inputs, snapshot their state (if any), and acknowledge.
   - Once JobMaster receives acknowledgments from all sinks, checkpoint is complete.

#### Characteristics:
- **Asynchronous:** Checkpointing happens concurrently with stream processing (minimal pause).
- **Incremental:** RocksDB backend supports incremental checkpoints (only changes since last checkpoint).
- **Exactly-Once:** Combined with two-phase commit sinks, ensures no data loss or duplication.

### 8.2 State Backends & Checkpoint Storage
- **State Backend:** Determines where state is stored during execution (Heap, RocksDB).
- **Checkpoint Storage:** Determines where checkpoints are persisted (JobManager memory, filesystem, S3).
  - `fs.default-scheme`: Points to HDFS, S3, or local filesystem.
  - Checkpoints stored as: `checkpoints/<job-id>/chk-<id>/`

### 8.3 Recovery Process
1. **Failure Detection:** TaskManager misses heartbeats → JobManager marks it as failed.
2. **Task Restart:** JobManager restarts failed tasks on healthy TaskManagers.
3. **State Restoration:** Tasks initialize state from latest successful checkpoint.
4. **Source Reset:** Sources reset to offsets/positions captured in checkpoint (e.g., Kafka consumer seeks to stored offset).
5. **Processing Resumes:** Jobs continue from exactly the point before failure.

### 8.4 High Availability (HA) for JobManager
- **Problem:** JobManager is a single point of failure.
- **Solution:** Run multiple JobManagers with leader election (via ZooKeeper or Kubernetes).
- **How HA Works:**
  - Standby JobManagers cluster synchronously replicate state from leader.
  - On leader failure, standby takes over and recovers jobs from persisted checkpoints.
  - Requires a highly available filesystem (HDFS, S3) for job metadata and checkpoints.

### 8.5 Checkpointing Tuning Parameters
- `execution.checkpointing.interval`: Time between checkpoints (default: 30000 ms).
- `execution.checkpointing.mode`: EXACTLY_ONCE or AT_LEAST_ONCE.
- `execution.checkpointing.timeout`: Time to wait for checkpoint completion (default: 600000 ms).
- `execution.checkpointing.min-pause`: Minimum pause between checkpoints (default: 30 s).
- `state.backend`: RocksDB or HashMap (default: RocksDB).
- `state.checkpoints.dir`: URI for checkpoint storage (e.g., `s3://flink-checkpoints/`).

---

## 9. Parallelism, Operator Chaining & Task Execution

Flink's parallelism model enables elastic scaling while preserving correctness.

### 9.1 Parallelism Levels
- **Cluster Parallelism:** Default parallelism for the cluster (set in `flink-conf.yaml`).
- **Job Parallelism:** Overrides cluster default for a specific job (set via `setParallelism()` or CLI `-p`).
- **Operator Parallelism:** Can be set per operator (e.g., `map().setParallelism(8)`).
- **Effective Parallelism:** The maximum parallelism among all operators in a job (dataflow must be able to redistribute data).

### 9.2 Dataflow Transformation & Operator Chaining
Flink's compiler applies several optimizations to the user's StreamGraph:

#### Chaining Rules:
Operators can be chained (fused into the same thread) if:
1. They belong to the same slot sharing group (default: all operators).
2. The upstream operator's output partitioning matches the downstream operator's input partitioning (or is reblockable).
3. Neither operator is an Async I/O operator or a operator that requires a barrier (like window).

#### Example Chains:
- `Source -> Map -> Filter` → Often chains into one task.
- `KeyBy -> Window -> Aggregate` → Usually chains (key grouping preserved).
- `Window -> ProcessFunction` → May not chain if ProcessFunction requires keyed state access.

#### Benefits of Chaining:
- Reduced thread context switching.
- Eliminated serialization/deserialization between chained operators.
- Lower network overhead (data passed via method calls).

### 9.3 Stream Partitioning & Rebalancing
When operators cannot be chained (e.g., due to key change or rescaling), Flink inserts a **rebalancing step** (data exchange).

#### Partitioning Strategies:
- **Forward:** No change in partitioning (used for chaining).
- **Rebalance / Random:** Uniformly redistributes data to all parallel instances.
- **Rescale:** Redistributes data to a subset of parallel instances (used when scaling up/down).
- **Partition By Hash / Key:** Groups data by key (used for `keyBy()`).
- **Broadcast:** Sends data to all downstream instances (used for broadcast state).

#### Rebalancing Process:
1. Upstream task serializes records and sends via Netty to downstream tasks.
2. Downstream task deserializes records and passes to operator.
3. Network buffers and credit flow control manage back pressure.

### 9.4 Task Execution Model
Each task slot runs one or more chains of operators in a single thread:
```
┌───────────────────────────────────────────────┐
│            Task Slot (Single Thread)          │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────┐ │
│  │ Source Op    │ │ Map Op       │ │ Sink Op  │ │
│  │ (Chained)    │ │ (Chained)    │ │ (Chained)│ │
│  └──────────────┘ └──────────────┘ └──────────┘ │
│                                                 │
│  ▶ Process:                                     │
│     1. Poll network buffer for input data       │
│     2. Deserialize records (if needed)          │
│     3. Execute chained operator chain           │
│     4. Serialize output records (if needed)     │
│     5. Push to network buffer for downstream    │
└───────────────────────────────────────────────┘
```

### 9.5 Back Pressure Monitoring & Propagation
- **Definition:** Back pressure occurs when downstream operators cannot consume data as fast as upstream produces it.
- **Detection:** Flink monitors the occupancy of task's input network buffers.
- **Levels:** OK, LOW, HIGH (exposed via Web UI and REST API).
- **Propagation:** Back pressure signals travel upstream via buffer occupancy, naturally throttling sources.

---

## 10. References & Further Reading

1. **Official Apache Flink Architecture Documentation:** [https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/)
2. **JobManager & TaskManager Details:** [https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/runtime/](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/runtime/)
3. **Task Slots & Resource Management:** [https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/task-slots/](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/task-slots/)
4. **Network Stack Deep Dive:** [https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack](https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack)
5. **Fault Tolerance & Checkpointing:** [https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/#checkpoints](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/#checkpoints)
6. **Deployment Guides:** [https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/)
7. **The Flink Paper (2015):** Carbone, P., et al. *"Apache Flink™: Stream and Batch Processing in a Single Engine."* IEEE Data Engineering Bulletin, 38(4), 28–38.
8. **Flink Forward Presentations:** [https://flink-forward.org/](https://flink-forward.org/) (search for architecture talks)