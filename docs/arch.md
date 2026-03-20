# MegaMon Architecture

MegaMon follows a strictly unidirectional flow of information to ensure consistency and scalability:
**State Observers -> Event Log -> Summary Producer -> Metrics**

## Components

### 1. State Observers
MegaMon uses two types of observers to monitor the cluster state:

- **Node Poller (`internal/aggregator/poller`)**
  - **Interface:** `ResourcePoller`
  - **Role:** Observes the state of GKE NodePools and Kubernetes Nodes on a regular interval.
  - **Logic:** It lists nodes and node pools, determines their "upness" based on conditions and TPU-specific labels, and passes this raw state to the **Event Log**.

- **Workload Reconciler (`internal/controller`)**
  - **Role:** Observes state changes of Kubernetes JobSets, LeaderWorkerSets (LWS), and Slices using the `controller-runtime` watch mechanism.
  - **Logic:** Whenever an event occurs, it triggers a unified reconciliation that lists all workloads, computes their expected state, and calls the **Event Log** to persist transitions.
  - **Durability:** It maintains a **Slice Owner Map** to ensure slices are correctly tracked even when they are temporarily missing from the API server. This map is persisted in a Kubernetes **ConfigMap** to ensure state recovery across controller restarts.

### 2. Event Log (`internal/aggregator/events`)
- **Interface:** `EventLog`
- **Role:** Acts as the system's memory. It manages state changes over time for every resource.
- **Current Observed Store:** An in-memory cache (`CurrentObservedStore`) that holds the latest resource observations. It decouples the watchers/pollers from the main aggregation loop, allowing the Aggregator to remain stateless while keeping the persistent GCS Event Store lean (omitting transient point-in-time attributes).
- **AppendStateChange:** When called by an observer, it fetches historical records from the **Event Store**, compares them with the new observation in the **Current Observed Store**, appends state change events (e.g., interruption, recovery) if necessary, and persists the updated log.
- **Event Store:** Backed by Google Cloud Storage (GCS), where logs are stored as JSON files partitioned by resource type.

### 3. Summary Producer (`internal/aggregator/report`)
- **Interface:** `SummaryProducer`
- **Role:** Operates on a summary interval to produce a global report.
- **Logic:** It fetches the full event log for all resources from the **Event Store** and calculates high-level reliability metrics, including:
  - **MTTR** (Mean Time to Recovery)
  - **MTBI** (Mean Time Between Interruptions)
  - **Total Uptime/Downtime**
  - **Interruption and Recovery Counts**

### 4. Metrics & Exporters (`internal/metrics`)
- **Role:** Consumes the global report produced by the **Summary Producer**.
- **Exporters:** Exposes metrics via OpenTelemetry (Prometheus format) or exports the raw report to a Kubernetes ConfigMap.

## High-Level Data Flow

```text
       [ Kubernetes API ]           [ GKE API ]
               ^                        |
    (Watch)    |             (Poll)     |
               |                        v
    +----------v----------+      +--------------+
    | Workload Reconciler |      |  Node Poller |
    +----------|----------+      +------|-------+
          |    |                        |
 (Persist)|    |   [ Raw Upness State ] |
          v    +-----------+------------+
    +-------------+        |
    | Slice Owner |        v
    |   Map (CM)  |  +-----------------+
    +-------------+  | Current Observed| (In-Memory)
                     |      Store      |
                     +--------|--------+
                              |
                              v
                     +-----------------+
                     |    Event Log    | <--- (AppendStateChange)
                     +--------|--------+
                           |
                           v
                 +-------------------+
                 |  GCS Event Store  |
                 +---------|---------+
                           |
                           | (Fetch All Records)
                           v
                 +-------------------+
                 |  Summary Producer |
                 +---------|---------+
                           |
                           v
                  +-----------------+
                  |  Global Report  |
                  +--------|--------+
                           |
           +---------------+---------------+
           |                               |
           v                               v
    +--------------+               +---------------+
    | OTEL Metrics |               | CMS Exporter  |
    | (Prometheus) |               | (ConfigMap)   |
    +--------------+               +---------------+
```

## Implementation Details

### Aggregator Coordination
The `Aggregator` (`internal/aggregator/aggregator.go`) acts as the central coordinator. It runs two main loops:
1. **Polling Loop:** Triggers the `NodePoller` to refresh node-related data.
2. **Aggregation Loop:** Triggers the `SummaryProducer` to generate the global report and subsequently calls all registered **Exporters**.

### Workload Reconciliation Trick
The `WorkloadReconciler` uses a `mapToStatic` handler to ensure that any change to a JobSet, LWS, or Slice triggers a full reconciliation of all workloads. This ensures that the Event Log for these resource types is always consistent and reflects the latest cluster state without waiting for a polling interval.
