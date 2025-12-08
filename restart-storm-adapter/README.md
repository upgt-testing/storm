# Storm Restart Testing Adapter

**Standalone Maven project** for restart testing Apache Storm's `LocalCluster` using the Restart Testing Framework.

## Overview

This is a **standalone Maven project** that integrates Apache Storm with the Restart Testing Framework. It does not participate in Storm's multi-module reactor build and can be built independently once Storm dependencies are available in your local Maven repository.

This adapter enables systematic testing of component failures and recoveries in Apache Storm's LocalCluster. It supports restarting Supervisors and Workers with different restart modes (GRACEFUL, CRASH, DELAYED_CRASH).

## Supported Components

| Component | Restart Support | Notes |
|-----------|----------------|-------|
| **Supervisors** | ✅ Full support | All restart modes supported |
| **Workers** | ✅ Full support | All restart modes supported |
| **Nimbus** | ❌ NOT supported | Architectural limitation (see below) |

## Limitations

### Nimbus Restart Not Supported

Nimbus cannot be restarted in LocalCluster because:
1. Nimbus is created during cluster construction
2. Deep integration with cluster state and ZooKeeper
3. No built-in mechanism to replace Nimbus instance

**Workaround**: To test Nimbus failures, close and recreate the entire LocalCluster.

```java
// Close existing cluster
cluster.close();

// Create new cluster with same configuration
cluster = new LocalCluster.Builder()
    .withDaemonConf(originalConf)
    .build();

// Resubmit topologies
cluster.submitTopology(name, conf, topology);
```

## Usage

### Basic Example

```java
import org.apache.storm.LocalCluster;
import org.apache.storm.restart.StormClusterAdapter;
import org.restarttest.core.RestartMode;

// Create adapter
StormClusterAdapter adapter = new StormClusterAdapter();

// Create cluster with simulated time
try (LocalCluster cluster = new LocalCluster.Builder()
        .withSimulatedTime()
        .withSupervisors(2)
        .build()) {

    // Submit topology
    cluster.submitTopology("test-topo", conf, topology);
    cluster.waitForIdle();

    // Restart first supervisor gracefully
    adapter.restartNode(cluster, "supervisor", 0, RestartMode.GRACEFUL);

    // Verify cluster is healthy
    HealthCheckResult health = adapter.getHealthCheck().checkHealth(cluster);
    assertTrue(health.isPassed());
}
```

### Restart Modes

The adapter supports three restart modes:

#### GRACEFUL
Cleanly shutdown workers before killing the supervisor:
```java
adapter.restartNode(cluster, "supervisor", 0, RestartMode.GRACEFUL);
```

#### CRASH
Immediate process termination without cleanup:
```java
adapter.restartNode(cluster, "supervisor", 0, RestartMode.CRASH);
```

#### DELAYED_CRASH
Kill the component, wait 30 seconds (simulated time), then restart:
```java
adapter.restartNode(cluster, "supervisor", 0, RestartMode.DELAYED_CRASH);
```

### State Capture and Verification

Capture cluster state before restart and verify invariants after:

```java
// Capture state before restart
ClusterState beforeState = adapter.getStateCapture().captureState(cluster);

// Restart component
adapter.restartNode(cluster, "supervisor", 0, RestartMode.GRACEFUL);

// Verify state consistency
ClusterState afterState = adapter.getStateCapture().captureState(cluster);
adapter.getStateCapture().verifyState(cluster, beforeState, afterState);
```

The state capture verifies:
- Topology count unchanged
- Topology statuses preserved (ACTIVE stays ACTIVE)
- Supervisor count unchanged

### Health Checks

Verify cluster health after restart:

```java
HealthCheckResult health = adapter.getHealthCheck().checkHealth(cluster);

if (health.isPassed()) {
    System.out.println("Cluster is healthy");
} else {
    System.out.println("Health check failures: " + health.getFailures());
}
```

Health checks verify:
- Cluster is responsive
- Nimbus has elected a leader
- Active topologies have executors assigned
- Cluster reaches idle state

### Restart All Nodes

Restart all nodes of a specific role:

```java
// Restart all supervisors
adapter.restartAllNodes(cluster, "supervisor", RestartMode.GRACEFUL);
```

## Integration with Simulated Time

The adapter is designed to work with Storm's simulated time for faster, deterministic tests:

```java
try (LocalCluster cluster = new LocalCluster.Builder()
        .withSimulatedTime()  // Enable simulated time
        .build()) {

    // Submit and wait for idle
    cluster.submitTopology("test", conf, topology);
    cluster.waitForIdle();

    // Restart operations automatically use simulated time
    adapter.restartNode(cluster, "supervisor", 0, RestartMode.DELAYED_CRASH);

    // Time is automatically advanced internally
}
```

## Maven Dependency

Add to your `pom.xml`:

```xml
<dependency>
    <groupId>org.apache.storm</groupId>
    <artifactId>restart-storm-adapter</artifactId>
    <version>2.8.4-SNAPSHOT</version>
    <scope>test</scope>
</dependency>

<dependency>
    <groupId>org.restarttest</groupId>
    <artifactId>restart-core</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <scope>test</scope>
</dependency>
```

## Examples

See `src/test/java/org/apache/storm/restart/StormClusterAdapterTest.java` for comprehensive integration test examples.

### Example: Test Supervisor Restart Preserves Topologies

```java
@Test
public void testSupervisorRestartPreservesTopologies() throws Exception {
    try (LocalCluster cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withSupervisors(2)
            .build()) {

        // Submit topology
        cluster.submitTopology("my-topology", new HashMap<>(), topology);
        cluster.waitForIdle();

        // Capture initial state
        ClusterState before = adapter.getStateCapture().captureState(cluster);

        // Restart supervisor
        adapter.restartNode(cluster, "supervisor", 0, RestartMode.GRACEFUL);

        // Verify topology still exists and is active
        ClusterState after = adapter.getStateCapture().captureState(cluster);
        adapter.getStateCapture().verifyState(cluster, before, after);

        // Verify cluster is healthy
        assertTrue(adapter.getHealthCheck().checkHealth(cluster).isPassed());
    }
}
```

## Architecture

### Components

1. **StormClusterAdapter** - Main adapter implementing `ClusterAdapter<LocalCluster>`
   - Handles supervisor and worker restarts
   - Manages restart modes
   - Coordinates with Storm's simulated time

2. **StormStateCapture** - Captures and verifies cluster state
   - Topology states (name, status, executor count)
   - Supervisor states (count, IDs)
   - Basic consistency checks

3. **StormHealthCheck** - Verifies cluster health
   - Cluster responsiveness
   - Nimbus leadership
   - Topology executor assignments
   - Cluster idle state

### Implementation Details

**Supervisor Restart**: Uses `LocalCluster.killSupervisor()` followed by `addSupervisor()` with preserved configuration.

**Worker Restart**: Uses `ProcessSimulator.killProcess()` and relies on Supervisor's auto-recovery mechanism.

**Simulated Time**: Integrates with `Time.advanceTimeSecs()` for delayed crash scenarios and `cluster.waitForIdle()` for synchronization.

## Testing

Run the integration tests:

```bash
mvn test -pl restart-storm-adapter
```

## Contributing

When adding new features:
1. Update state capture if new state needs verification
2. Add health checks for new components
3. Add integration tests for new scenarios
4. Update this README with examples

## License

Apache License 2.0 - See Storm project license
