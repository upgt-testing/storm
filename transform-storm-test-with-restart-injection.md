# Prompt: Transform Storm LocalCluster Test with Restart Position Injection

## Objective

Transform an existing Apache Storm LocalCluster test to inject restart positions for distributed system restart testing. The transformation will generate:
1. A new test file with `_RestartInjected` suffix
2. A restart configuration file for the Maven plugin

## Input

- **Test File Path**: Path to the original test file (e.g., `/path/to/TestStormTopology.java`)
- **Test Class**: Fully-qualified class name (e.g., `org.apache.storm.topology.TestStormTopology`)

## Output

1. **Generated Test File**: `{OriginalFileName}_RestartInjected.java` at the same directory as the input file
2. **Restart Configuration**: `restart-config.json` in the `restarts-config/` directory under the same module directory as the test file, if not exist create it.

## Transformation Instructions

### Step 1: Analyze the Original Test

Read the input test file and identify:

1. **Cluster Setup**: Find the LocalCluster instance variable (e.g., `LocalCluster cluster` or `LocalCluster localCluster`)
   - **CRITICAL**: The cluster object may be inherited from a parent base test class rather than being directly initialized in the test class itself
   - **IMPORTANT**: Carefully check the class hierarchy (look for `extends` clauses) and examine parent classes for cluster initialization
   - Common patterns:
     - Direct initialization: `private LocalCluster cluster;` in the test class
     - Inherited from parent: `extends StormTestBase` where parent has the cluster field
     - Static shared cluster: `protected static LocalCluster cluster;` in base class
     - Field with different name: `cluster`, `localCluster`, `stormCluster`, etc.
   - **Strategy**: If the cluster is not found in the test class, traverse up the inheritance hierarchy to locate it
2. **Test Methods**: Identify all `@Test` annotated methods
3. **Critical Operations**: Look for operations that involve state transitions, such as:
   - Topology operations: `submitTopology()`, `killTopology()`, `activate()`, `deactivate()`, `rebalance()`
   - Tuple processing: Spout emission, bolt processing, tuple acknowledgment
   - State operations: State updates, state queries, checkpoint operations
   - DRPC operations: DRPC function submission, DRPC execution
   - Trident operations: Batch processing, aggregations, state updates
   - Time operations: Advancing simulated time, waiting for idle
   - Worker operations: Worker startup, worker shutdown, worker failover
   - Supervisor operations: Supervisor heartbeats, supervisor assignment

### Step 2: Identify Restart Points

For each test method, identify potential restart points based on these criteria:

**Good Restart Points** (inject here):
- After topology submission but before activation
- After tuple emission from spouts
- During bolt processing (between tuples)
- After state updates in stateful bolts
- After checkpoint/commit operations
- During DRPC execution
- After Trident batch processing
- Before/after time advancement (in simulated time tests)
- After worker assignment
- During tuple acking/failing
- After topology activation/deactivation
- Before topology kill operations

**Poor Restart Points** (avoid):
- Before cluster setup (no cluster exists yet)
- After cluster shutdown (cluster already destroyed)
- During trivial operations (simple getters with no state changes)
- Operations that are too fast to test meaningful state

**Naming Convention for Restart Positions**:
- Use descriptive, lowercase names with underscores
- Pattern: `{operation}_{context}`
- Examples:
  - `after_topology_submit`
  - `after_spout_emit`
  - `during_bolt_process`
  - `after_state_update`
  - `after_checkpoint`
  - `during_drpc_execute`
  - `after_batch_process`
  - `before_topology_kill`
  - `after_worker_assign`
  - `during_tuple_ack`
  - `after_activate`
  - `before_deactivate`

### Step 3: Generate the Restart-Injected Test File

Create a new test file with the following transformations:

#### 3.1 Package and Imports

```java
// Keep original package declaration
package org.apache.storm.topology;

// Add these imports at the top (if not already present)
import org.restarttest.api.RestartFramework;
import org.restarttest.core.RestartMode;

// Keep all original imports
```

#### 3.2 Class Declaration

```java
// Original class name: TestStormTopology
// New class name: TestStormTopology_RestartInjected

// Keep the same inheritance structure!
// If original: public class TestStormTopology extends StormTestBase
// Then use:    public class TestStormTopology_RestartInjected extends StormTestBase
public class TestStormTopology_RestartInjected {
    // Keep all original fields and variables
}
```

**CRITICAL**: If the test class extends a base class, preserve this inheritance in the generated test. The cluster object may be defined in the parent class. You must carefully trace through the inheritance hierarchy to find the cluster object.

#### 3.3 Cluster Setup and Teardown

Keep the `@Before`/`@BeforeAll`/`@BeforeEach` and `@After`/`@AfterAll`/`@AfterEach` methods unchanged:

```java
@Before
public void setUp() throws Exception {
    // Keep original setup code unchanged
}

@After
public void tearDown() throws Exception {
    // Keep original teardown code unchanged
}
```

**Note**: Setup methods may be inherited from parent class. In that case, don't add setup methods to the generated test. Carefully examine the parent class to understand the cluster initialization pattern.

#### 3.4 Transform Test Methods

For each `@Test` method, apply the following transformations:

**Original Test Method**:
```java
@Test
public void testTopologyExecution() throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", new TestWordSpout(), 1);
    builder.setBolt("bolt", new TestWordCounter(), 2)
           .shuffleGrouping("spout");

    Map<String, Object> conf = new HashMap<>();
    cluster.submitTopology("test-topology", conf, builder.createTopology());

    cluster.advanceClusterTime(10);

    Map result = Testing.readTuples(cluster, "bolt");
    assertTrue(result.size() > 0);
}
```

**Transformed Test Method**:
```java
@Test
public void testTopologyExecution() throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", new TestWordSpout(), 1);
    builder.setBolt("bolt", new TestWordCounter(), 2)
           .shuffleGrouping("spout");

    Map<String, Object> conf = new HashMap<>();
    cluster.submitTopology("test-topology", conf, builder.createTopology());

    // RESTART POINT 1: after_topology_submit
    RestartFramework.at("after_topology_submit")
        .on(cluster)  // Use the cluster instance (may be from parent class)
        .restart("supervisor")
        .withIndex(0)
        .withMode(RestartMode.GRACEFUL)
        .execute();

    cluster.advanceClusterTime(5);

    // RESTART POINT 2: during_processing
    RestartFramework.at("during_processing")
        .on(cluster)
        .restart("worker")
        .withIndex(0)
        .withMode(RestartMode.GRACEFUL)
        .execute();

    cluster.advanceClusterTime(5);

    // RESTART POINT 3: before_result_check
    RestartFramework.at("before_result_check")
        .on(cluster)
        .restart("supervisor")
        .withIndex(0)
        .withMode(RestartMode.GRACEFUL)
        .execute();

    Map result = Testing.readTuples(cluster, "bolt");
    assertTrue(result.size() > 0);
}
```

**Injection Pattern**:

1. **After Topology Operations**:
   ```java
   cluster.submitTopology("test", conf, topology);

   // Inject restart point
   RestartFramework.at("after_topology_submit")
       .on(cluster)
       .restart("supervisor")
       .withIndex(0)
       .withMode(RestartMode.GRACEFUL)
       .execute();
   ```

2. **During Processing**:
   ```java
   cluster.advanceClusterTime(5);

   // Inject restart point
   RestartFramework.at("during_processing")
       .on(cluster)
       .restart("worker")
       .withIndex(0)
       .withMode(RestartMode.GRACEFUL)
       .execute();
   ```

3. **Before Critical Operations**:
   ```java
   // Inject restart point before kill
   RestartFramework.at("before_topology_kill")
       .on(cluster)
       .restart("supervisor")
       .withIndex(0)
       .withMode(RestartMode.GRACEFUL)
       .execute();

   cluster.killTopology("test");
   ```

4. **After State Updates**:
   ```java
   // State update happened
   updateState(key, value);

   // Inject restart point
   RestartFramework.at("after_state_update")
       .on(cluster)
       .restart("worker")
       .withIndex(0)
       .withMode(RestartMode.GRACEFUL)
       .execute();
   ```

#### 3.5 Storm-Specific Node Roles

Storm LocalCluster has two primary node types that support restart:

- **`supervisor`**: Manages worker processes, handles worker assignment and lifecycle
- **`worker`**: Executes topology components (spouts/bolts), handles tuple processing

**IMPORTANT**: Nimbus restart is NOT supported in LocalCluster due to architectural limitations. The adapter will throw `UnsupportedOperationException` if you attempt to restart Nimbus.

**Default Restart Configuration**:
Use these defaults for all injected restart points:
- **For topology lifecycle operations**: `"supervisor"` (manages topology assignment)
- **For tuple processing operations**: `"worker"` (executes spout/bolt logic)
- **For state operations**: `"worker"` (maintains component state)
- **For cluster-wide operations**: Both `"supervisor"` and `"worker"`
- **Node Index**: `0` (first node)
- **Restart Mode**: `RestartMode.GRACEFUL` (default, safest)

#### 3.6 Node Role Selection Guidelines

| Operation Type | Primary Node Role | Secondary Node Role | Reason |
|----------------|-------------------|---------------------|--------|
| Topology submission | `supervisor` | - | Supervisor assigns workers |
| Topology activation/deactivation | `supervisor` | - | Supervisor manages lifecycle |
| Topology kill | `supervisor` | - | Supervisor handles shutdown |
| Tuple emission | `worker` | - | Workers execute spouts |
| Tuple processing | `worker` | - | Workers execute bolts |
| State updates | `worker` | - | Workers maintain state |
| Tuple acking | `worker` | - | Workers handle acking |
| DRPC execution | `worker` | `supervisor` | Workers execute, supervisor coordinates |
| Trident batch processing | `worker` | - | Workers process batches |
| Worker assignment | `supervisor` | - | Supervisor assigns workers |
| Rebalancing | `supervisor` | `worker` | Supervisor reassigns, workers restart |
| Time advancement | Both | - | Both participate in simulated time |

### Step 4: Generate Restart Configuration File

Create `restarts-config/restart-config.json` with the following structure:

```json
{
  "tests": [
    {
      "testClass": "org.apache.storm.topology.TestStormTopology_RestartInjected",
      "testMethod": "testTopologyExecution",
      "restartPoints": [
        {
          "position": "after_topology_submit",
          "targets": ["supervisor"],
          "modes": ["GRACEFUL", "CRASH"]
        },
        {
          "position": "during_processing",
          "targets": ["worker", "supervisor"],
          "modes": ["GRACEFUL", "CRASH", "DELAYED_CRASH"]
        },
        {
          "position": "before_result_check",
          "targets": ["supervisor"],
          "modes": ["GRACEFUL", "CRASH"]
        }
      ]
    }
  ]
}
```

#### Configuration Generation Rules

For each test method in the transformed test:

1. **Create a test specification** with:
   - `testClass`: The fully-qualified name of the generated test class
   - `testMethod`: The test method name (same as original)
   - `restartPoints`: Array of restart point configurations

2. **For each restart point** injected in the test method:
   - `position`: The position identifier used in `.at("...")`
   - `targets`: Array of node roles to test
   - `modes`: Array of restart modes to test

#### Target Selection for Storm Operations

**Supervisor-only operations** (`["supervisor"]`):
- Topology submission
- Topology activation/deactivation
- Topology kill
- Worker assignment
- Rebalancing coordination
- Supervisor heartbeats

**Worker-only operations** (`["worker"]`):
- Tuple emission from spouts
- Tuple processing in bolts
- State updates in stateful bolts
- Tuple acking/failing
- Component-level operations
- Local state management

**Both Supervisor and Worker** (`["supervisor", "worker"]`):
- Topology rebalancing
- Time advancement (simulated time)
- Cluster-wide state transitions
- Multi-component coordination
- Failure recovery scenarios

#### Mode Selection Guidelines

- **`["GRACEFUL"]`**: Basic test, verify restart works
  - Use for: Initial testing, simple state transitions

- **`["GRACEFUL", "CRASH"]`**: Standard test, verify crash recovery
  - Use for: Topology operations, tuple processing, state operations

- **`["GRACEFUL", "CRASH", "DELAYED_CRASH"]`**: Advanced test, verify timing-sensitive operations
  - Use for: Tuple acking, state checkpoints, distributed coordination, exactly-once semantics, Trident batches

### Step 5: File Placement

1. **Generated Test File**:
   - Location: Same directory as original test file
   - Name: `{OriginalClassName}_RestartInjected.java`
   - Example: `TestStormOps.java` → `TestStormOps_RestartInjected.java`

2. **Restart Configuration**:
   - Location: `restarts-config/` directory under the same module directory as the test file
   - Name: `restart-config.json`
   - If file exists, append to the `tests` array (avoid duplicates)
   - If file doesn't exist, create new file

## Example Transformation

### Input: `TestWordCount.java`

```java
package org.apache.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TestWordCount {
    private LocalCluster cluster;

    @Before
    public void setUp() throws Exception {
        cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(2)
                .withPortsPerSupervisor(3)
                .build();
    }

    @After
    public void tearDown() throws Exception {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Test
    public void testWordCount() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestWordSpout(), 1);
        builder.setBolt("counter", new TestWordCounter(), 2)
               .shuffleGrouping("spout");

        Map<String, Object> conf = new HashMap<>();
        cluster.submitTopology("word-count", conf, builder.createTopology());

        cluster.advanceClusterTime(10);

        Map result = Testing.readTuples(cluster, "counter");
        assertTrue(result.size() > 0);
    }
}
```

### Output 1: `TestWordCount_RestartInjected.java`

```java
package org.apache.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.restarttest.api.RestartFramework;
import org.restarttest.core.RestartMode;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TestWordCount_RestartInjected {
    private LocalCluster cluster;

    @Before
    public void setUp() throws Exception {
        cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(2)
                .withPortsPerSupervisor(3)
                .build();
    }

    @After
    public void tearDown() throws Exception {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Test
    public void testWordCount() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestWordSpout(), 1);
        builder.setBolt("counter", new TestWordCounter(), 2)
               .shuffleGrouping("spout");

        Map<String, Object> conf = new HashMap<>();
        cluster.submitTopology("word-count", conf, builder.createTopology());

        // RESTART POINT 1: after_topology_submit
        RestartFramework.at("after_topology_submit")
            .on(cluster)
            .restart("supervisor")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        cluster.advanceClusterTime(5);

        // RESTART POINT 2: during_processing
        RestartFramework.at("during_processing")
            .on(cluster)
            .restart("worker")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        cluster.advanceClusterTime(5);

        // RESTART POINT 3: before_result_check
        RestartFramework.at("before_result_check")
            .on(cluster)
            .restart("supervisor")
            .withIndex(0)
            .withMode(RestartMode.GRACEFUL)
            .execute();

        Map result = Testing.readTuples(cluster, "counter");
        assertTrue(result.size() > 0);
    }
}
```

### Output 2: `restarts-config/restart-config.json`

```json
{
  "tests": [
    {
      "testClass": "org.apache.storm.topology.TestWordCount_RestartInjected",
      "testMethod": "testWordCount",
      "restartPoints": [
        {
          "position": "after_topology_submit",
          "targets": ["supervisor"],
          "modes": ["GRACEFUL", "CRASH"]
        },
        {
          "position": "during_processing",
          "targets": ["worker", "supervisor"],
          "modes": ["GRACEFUL", "CRASH", "DELAYED_CRASH"]
        },
        {
          "position": "before_result_check",
          "targets": ["supervisor"],
          "modes": ["GRACEFUL", "CRASH"]
        }
      ]
    }
  ]
}
```

## Storm-Specific Patterns

### Pattern 1: Topology Lifecycle Testing

```java
@Test
public void testTopologyLifecycle() throws Exception {
    // Submit
    cluster.submitTopology("test", conf, topology);

    RestartFramework.at("after_submit")
        .on(cluster)
        .restart("supervisor")
        .execute();

    // Activate
    cluster.activate("test");

    RestartFramework.at("after_activate")
        .on(cluster)
        .restart("supervisor")
        .execute();

    // Deactivate
    cluster.deactivate("test");

    RestartFramework.at("after_deactivate")
        .on(cluster)
        .restart("supervisor")
        .execute();

    // Kill
    cluster.killTopology("test");
}
```

### Pattern 2: Tuple Processing Testing

```java
@Test
public void testTupleProcessing() throws Exception {
    cluster.submitTopology("test", conf, topology);

    // Let some tuples process
    cluster.advanceClusterTime(5);

    RestartFramework.at("during_emit")
        .on(cluster)
        .restart("worker")
        .execute();

    // Process more tuples
    cluster.advanceClusterTime(5);

    RestartFramework.at("during_ack")
        .on(cluster)
        .restart("worker")
        .execute();

    // Verify results
    Map result = Testing.readTuples(cluster, "bolt");
    assertFalse(result.isEmpty());
}
```

### Pattern 3: Stateful Bolt Testing

```java
@Test
public void testStatefulBolt() throws Exception {
    cluster.submitTopology("stateful-test", conf, topology);

    // Process some tuples to build state
    cluster.advanceClusterTime(5);

    RestartFramework.at("after_state_build")
        .on(cluster)
        .restart("worker")
        .execute();

    // Update state
    cluster.advanceClusterTime(5);

    RestartFramework.at("after_state_update")
        .on(cluster)
        .restart("worker")
        .withMode(RestartMode.CRASH)
        .execute();

    // Verify state recovered
    cluster.advanceClusterTime(5);
    Map result = Testing.readTuples(cluster, "stateful-bolt");
    verifyStateConsistency(result);
}
```

### Pattern 4: Trident Batch Testing

```java
@Test
public void testTridentBatch() throws Exception {
    TridentTopology topology = new TridentTopology();
    // Build Trident topology

    cluster.submitTopology("trident-test", conf, topology.build());

    // Process first batch
    cluster.advanceClusterTime(100);

    RestartFramework.at("after_batch_1")
        .on(cluster)
        .restart("worker")
        .execute();

    // Process second batch
    cluster.advanceClusterTime(100);

    RestartFramework.at("after_batch_2")
        .on(cluster)
        .restart("worker")
        .withMode(RestartMode.DELAYED_CRASH)
        .execute();

    // Verify batches
    verifyBatchResults();
}
```

### Pattern 5: Rebalancing Testing

```java
@Test
public void testRebalance() throws Exception {
    cluster.submitTopology("rebalance-test", conf, topology);
    cluster.advanceClusterTime(10);

    RestartFramework.at("before_rebalance")
        .on(cluster)
        .restart("supervisor")
        .execute();

    // Trigger rebalance
    RebalanceOptions opts = new RebalanceOptions();
    opts.set_num_workers(4);
    cluster.rebalance("rebalance-test", opts);

    RestartFramework.at("during_rebalance")
        .on(cluster)
        .restart("worker")
        .execute();

    cluster.advanceClusterTime(10);

    RestartFramework.at("after_rebalance")
        .on(cluster)
        .restart("supervisor")
        .execute();

    // Verify topology still works
    verifyTopologyHealth();
}
```

### Pattern 6: DRPC Testing

```java
@Test
public void testDRPC() throws Exception {
    LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
    // Build DRPC topology

    cluster.submitTopology("drpc-test", conf, builder.createLocalTopology());

    RestartFramework.at("after_topology_start")
        .on(cluster)
        .restart("supervisor")
        .execute();

    // Execute DRPC function
    String result = drpc.execute("exclamation", "hello");

    RestartFramework.at("after_drpc_execute")
        .on(cluster)
        .restart("worker")
        .execute();

    assertEquals("hello!", result);
}
```

## Validation Checklist

After transformation, verify:

- [ ] Generated test file compiles without errors
- [ ] All original test logic is preserved
- [ ] Restart points are placed at meaningful Storm operations
- [ ] Restart position names are descriptive and Storm-specific
- [ ] Node roles (supervisor/worker) are correctly chosen
- [ ] No attempts to restart Nimbus (unsupported)
- [ ] Configuration file has correct fully-qualified class names
- [ ] Configuration file includes all restart points from the test
- [ ] Target arrays match the operation type
- [ ] Mode arrays are appropriate for timing sensitivity
- [ ] Files are placed in correct locations
- [ ] Original test file is not modified (only new files created)
- [ ] **CRITICAL**: Cluster object is correctly identified (may be from parent class)
- [ ] Inheritance structure is preserved if test extends a base class

## Advanced Scenarios

### Multiple Test Methods

If the original test has multiple `@Test` methods:

1. Transform each method independently
2. Inject restart points in each method
3. Create a separate test specification for each method in the configuration

Example configuration:
```json
{
  "tests": [
    {
      "testClass": "org.apache.storm.TestStorm_RestartInjected",
      "testMethod": "testTopologySubmit",
      "restartPoints": [...]
    },
    {
      "testClass": "org.apache.storm.TestStorm_RestartInjected",
      "testMethod": "testTupleProcessing",
      "restartPoints": [...]
    }
  ]
}
```

### Helper Methods

If the test has helper methods:

1. **Do not inject restart points in helper methods**
2. Only inject in `@Test` annotated methods
3. Keep helper methods unchanged

### Cluster Object in Parent Class

**CRITICAL**: A common scenario in Storm tests is that the `LocalCluster` object is defined in a parent base class, not in the test class itself.

**Step-by-step approach to locate the cluster object**:

1. Check if the test class extends a base class (look for `extends` keyword)
2. If yes, examine the parent class for cluster fields
3. Look for common field names: `cluster`, `localCluster`, `stormCluster`
4. Check if the field is `private`, `protected`, or `public` - you may need to use the same access pattern
5. In the transformed test, use the inherited cluster object for restart injection

Example:
```java
// Parent class: StormTestBase.java
public abstract class StormTestBase {
    protected static LocalCluster cluster;

    @BeforeAll
    public static void setupCluster() throws Exception {
        cluster = new LocalCluster.Builder()
                .withSimulatedTime()
                .withSupervisors(2)
                .build();
    }
}

// Test class: TestStormTopology.java
public class TestStormTopology extends StormTestBase {
    @Test
    public void testTopology() throws Exception {
        // cluster is inherited from parent
        cluster.submitTopology("test", conf, topology);
    }
}

// Transformed test: TestStormTopology_RestartInjected.java
public class TestStormTopology_RestartInjected extends StormTestBase {
    @Test
    public void testTopology() throws Exception {
        RestartFramework.at("before_submit")
            .on(cluster)  // Use inherited cluster object
            .restart("supervisor")
            .execute();

        cluster.submitTopology("test", conf, topology);
    }
}
```

### Tests Without Obvious Restart Points

If a test has no clear state transitions:

1. Inject restart points within the range of (a) after cluster setup and (b) before cluster teardown
2. Evenly distribute restart points to cover the test execution
3. You MUST use percentage-based positions (e.g., `at_25_percent`, `at_50_percent`) to at least cover 4 points during the test execution
4. Find cluster operations to place restart points around

**IMPORTANT**: You are NOT allowed to skip any test transformation due to lack of restart points. Always inject at least one restart point per test method.

## Common Storm Operations and Suggested Restart Points

| Storm Operation | Suggested Restart Point Name | Node Role | Timing |
|-----------------|------------------------------|-----------|--------|
| `submitTopology()` | `after_topology_submit` | `supervisor` | After |
| `killTopology()` | `before_topology_kill` | `supervisor` | Before |
| `activate()` | `after_activate` | `supervisor` | After |
| `deactivate()` | `after_deactivate` | `supervisor` | After |
| `rebalance()` | `before_rebalance`, `after_rebalance` | `supervisor`, `worker` | Before/After |
| Spout emit | `after_spout_emit` | `worker` | After |
| Bolt execute | `during_bolt_execute` | `worker` | During |
| Tuple ack | `after_tuple_ack` | `worker` | After |
| Tuple fail | `after_tuple_fail` | `worker` | After |
| State update | `after_state_update` | `worker` | After |
| Checkpoint | `after_checkpoint` | `worker` | After |
| DRPC execute | `during_drpc_execute` | `worker` | During |
| Trident batch | `after_batch_process` | `worker` | After |
| Time advance | `after_time_advance` | Both | After |
| Worker assign | `after_worker_assign` | `supervisor` | After |

## Notes

- **Non-invasive**: Original test file is never modified
- **Incremental**: Can transform tests one at a time
- **Compatible**: Generated tests can run both with and without restart injection
- **Configurable**: Configuration file allows easy adjustment of test matrix
- **Storm-aware**: Node role selection is specific to Storm architecture (Supervisor vs Worker)
- **Nimbus limitation**: Nimbus restart is not supported in LocalCluster due to architectural constraints
- **CRITICAL**: Always carefully check for cluster objects in parent base classes

## Dependencies

Ensure the following dependencies are included in the test's module to use the Restart Testing Framework:

```xml
<dependencies>
    <!-- Existing dependencies... -->

    <!-- Restart Testing Framework - Core -->
    <dependency>
        <groupId>org.restarttest</groupId>
        <artifactId>restart-core</artifactId>
        <version>1.0.0-SNAPSHOT</version>
        <scope>test</scope>
    </dependency>

    <!-- Restart Testing Framework - Storm Adapter -->
    <dependency>
        <groupId>org.restarttest</groupId>
        <artifactId>restart-storm-adapter</artifactId>
        <version>1.0.0-SNAPSHOT</version>
        <scope>test</scope>
    </dependency>
</dependencies>
```

## Build Requirements

**IMPORTANT**: Storm tests require Java 17 and special Maven options to build and compile.

Before building or running Storm tests with restart injection, you need to:

### 1. Build the Restart Storm Adapter (Standalone)

The restart-storm-adapter is maintained as a standalone project and must be built separately first:

```bash
# Navigate to the restart-storm-adapter directory
cd restart-storm-adapter/

# Ensure Java 17 is being used
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64  # Adjust path for your system
export MAVEN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED"

# Build and install the adapter to local Maven repository
mvn clean install -DskipTests

# Return to Storm root directory
cd ..
```

### 2. Build Storm Modules with Restart Testing Support

After the restart-storm-adapter is installed to your local Maven repository, you can build the Storm modules that contain the tests:

```bash
# Set required environment variables
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64  # Adjust path for your system
export MAVEN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED"

# Build the storm-server module (contains MessagingTest, TestRebalance, TestingTest, TickTupleTest, LocalNimbusTest)
cd storm-server/
mvn clean compile

# Build the storm-core module (contains integration tests)
cd ../storm-core/
mvn clean compile

# Or build all Storm modules from the root
cd ..
mvn clean compile -pl storm-core,storm-server
```

### 3. Run Tests with Restart Injection

```bash
# Run a specific test with restart injection
mvn test -Dtest=TestClassName_RestartInjected

# Run all restart-injected tests
mvn test -Dtest="*_RestartInjected"
```

### Important Notes

- **Java 17 Required**: Storm 2.8.3 is compiled with Java 17 (class version 61.0)
- **MAVEN_OPTS**: The `--add-opens java.base/java.lang=ALL-UNNAMED` flag is required for Storm's internal reflection operations to work properly with Java 17's module system
- **Standalone Adapter**: The restart-storm-adapter is NOT a module of the Storm project. It must be built separately and installed to the local Maven repository before building Storm modules
- **Dependencies**: The following modules have restart testing dependencies added:
  - `storm-server`: Contains tests for MessagingTest, TestRebalance, TestingTest, TickTupleTest, LocalNimbusTest
  - `storm-core`: Contains tests for TopologyIntegrationTest, NettyIntegrationTest, and integration TestingTest

### Verifying Java Version

```bash
# Check current Java version
java -version  # Should show Java 17 (e.g., openjdk version "17.0.x")

# If using multiple Java versions, set JAVA_HOME explicitly
# For Ubuntu/Debian:
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# For macOS with Homebrew:
export JAVA_HOME=/usr/local/opt/openjdk@17

# For SDKMAN:
sdk use java 17.x.x
```
