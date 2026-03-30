# MegaMon Architecture

MegaMon follows a strictly unidirectional flow of information to ensure consistency and scalability:
**State Observers -> Event Log -> Summary Producer -> Metrics**

## Components

### 1. State Observers
MegaMon uses two main types of observers to monitor the cluster state:

#### A. Pollers
- **Node Poller (`internal/aggregator/poller`)**
  - **Interface:** `ResourcePoller`
  - **Role:** Observes the state of GKE NodePools and Kubernetes Nodes on a regular interval.
  - **Logic:** It lists nodes and node pools, determines their "upness" based on conditions and TPU-specific labels, and passes this raw state to the **Event Log**.

#### B. Reconcilers
- **Workload Reconciler (`internal/controller`)**
  - **Role:** Observes state changes of Kubernetes JobSets, LeaderWorkerSets (LWS), and Slices using the `controller-runtime` watch mechanism.
  - **Logic:** Whenever an event occurs, it triggers a unified reconciliation that delegates logic to resource-specific processors (`processJobSets`, `processLeaderWorkerSets`, `processSlices`). These processors compute expected vs. actual upness and call the **Event Log** to persist transitions in a single atomic batch.
  - **Durability:** It utilizes a **Slice Owner Map**, stored in a Kubernetes **ConfigMap** and accessed via the `controller-runtime` cache, to ensure slices are correctly tracked even when they are temporarily missing from the API server. This allows the reconciler to remain stateless while ensuring state recovery across restarts.

- **Pod Reconciler (`internal/controller/pod_reconciler.go`)**
  - **Role:** Has a limited, specific role to discover where workloads are physically scheduled. It watches JobSet leader Pods to dynamically map Jobs/JobSets to specific GKE NodePools.
  - **Logic:** It populates the in-memory `NodePoolScheduling` mapping within the Aggregator (which is eventually merged directly into the Global Report). It can also optionally label the Kubernetes `Job` with the scheduled node pool. Unlike the Workload Reconciler, it does *not* write state changes to the Event Log.

### 2. Event Log (`internal/aggregator/events`)
- **Interface:** `EventLog`
- **Role:** Acts as the system's memory. It manages state changes over time for every resource.
- **Current Observed Store:** An in-memory cache (`CurrentObservedStore`) that holds the latest resource observations. It decouples the watchers/pollers from the main aggregation loop, allowing the Aggregator to remain stateless while keeping the persistent GCS Event Store lean (omitting transient point-in-time attributes).
- **AppendStateChange(s):** When called by an observer, it fetches historical records from the **Event Store**, compares them with the new observation in the **Current Observed Store**, appends state change events (e.g., interruption, recovery) if necessary, and persists the updated log. It supports both single resource updates and atomic batch updates of multiple resource types.
- **Event Store:** Backed by Google Cloud Storage (GCS), where logs are stored as JSON files partitioned by resource type.

### 3. Summary Producer (`internal/aggregator/report`)
- **Interface:** `SummaryProducer`
- **Role:** Operates on a summary interval to produce a global report.
- **Logic:** It fetches historical event logs from the **Event Store** and the latest resource state (including transient `Attrs`) from the **Current Observed Store** (via the **Event Log**) to calculate high-level reliability metrics:
  - **MTTR** (Mean Time to Recovery)
  - **MTBI** (Mean Time Between Interruptions)
  - **Total Uptime/Downtime**
  - **Interruption and Recovery Counts**

### 4. Metrics & Exporters (`internal/metrics`)
- **Role:** Consumes the global report produced by the **Summary Producer**.
- **Exporters:** Exposes metrics via OpenTelemetry (Prometheus format) or exports the raw report to a Kubernetes ConfigMap.

## High-Level Data Flow

```text
     [ GKE API ]                               [ Kubernetes API ]
          |                                            |
       (Poll)                                       (Watch)
          |                                            |
          |                      +---------------------+---------------------+
          v                      |                                           |
  +---------------+    +---------v---------+                       +---------v---------+
  |               |    |                   |                       |                   |
  |  Node Poller  |    |  Pod Reconciler   |                       |Workload Reconciler| <---> [ ConfigMap ]
  |               |    |                   |                       |                   |   (Slice Owner Map)
  +-------+-------+    +---------+---------+                       +---------+---------+
          |                      |                                           |
          |                      v                                           |
          |            +-------------------+                                 |
          |            |     NodePool      |                                 |
          |            |  Scheduling Map   |                                 |
          |            +---------+---------+                                 |
          |                      |                                           |
          +----------------------+--------------------+                      |
                                 |                    |                      |
  [ Raw Upness State ]           |                    | [ Raw Upness State ] |
                                 |                    |                      |
                                 |                    v                      v
                                 |        +------------------------------------------------+
                                 |        |                   Event Log                    |
                                 |        |                                                |
                                 |        |  +--------------------+  +------------------+  |
                                 |        |  |  Current Observed  |  | GCS Event Store  |  |
                                 |        |  |  Store (In-Memory) |  |   (Persistent)   |  |
                                 |        |  +---------+----------+  +--------+---------+  |
                                 |        |            |                      |            |
                                 |        +------------|----------------------|------------+
                                 |                     |                      |
                                 |             (Fetch Observed)         (Fetch Events)
                                 |                     |                      |
                                 |                     v                      v
                                 |        +------------------------------------------------+
                                 |        |                Summary Producer                |
                                 |        +----------------------+-------------------------+
                                 |                               |
                                 | (Merge)                       | (Base Report)
                                 +--------------------+          |
                                                      |          |
                                                      v          v
                                          +------------------------------------------------+
                                          |                 Global Report                  |
                                          +----------------------+-------------------------+
                                                                 |
                                                 +---------------+---------------+
                                                 |                               |
                                                 v                               v
                                          +---------------+               +---------------+
                                          | OTEL Metrics  |               | CMS Exporter  |
                                          | (Prometheus)  |               |  (ConfigMap)  |
                                          +---------------+               +---------------+
```

## Implementation Details

### Aggregator Coordination
The `Aggregator` (`internal/aggregator/aggregator.go`) acts as the central coordinator. It runs two main loops:
1. **Polling Loop:** Triggers the `NodePoller` to refresh node-related data.
2. **Aggregation Loop:** Triggers the `SummaryProducer` to generate the global report and subsequently calls all registered **Exporters**.

### Workload Reconciliation Trick
The `WorkloadReconciler` uses a `mapToStatic` handler to ensure that any change to a JobSet, LWS, or Slice triggers a full reconciliation of all workloads. This ensures that the Event Log for these resource types is always consistent and reflects the latest cluster state without waiting for a polling interval.
