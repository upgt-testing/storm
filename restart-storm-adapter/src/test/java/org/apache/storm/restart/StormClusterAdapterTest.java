/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.restart;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.restarttest.core.RestartMode;
import org.restarttest.health.HealthCheckResult;
import org.restarttest.state.ClusterState;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for StormClusterAdapter.
 */
public class StormClusterAdapterTest {

    private LocalCluster cluster;
    private StormClusterAdapter adapter;

    @BeforeEach
    public void setUp() throws Exception {
        adapter = new StormClusterAdapter();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (cluster != null) {
            cluster.shutdown();
            cluster = null;
        }
    }

    @Test
    public void testGetClusterType() {
        assertEquals(LocalCluster.class, adapter.getClusterType());
    }

    @Test
    public void testSupervisorGracefulRestart() throws Exception {
        cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withSupervisors(2)
            .withPortsPerSupervisor(3)
            .build();

        // Submit test topology
        Map<String, Object> topoConf = new HashMap<>();
        cluster.submitTopology("test-topology", topoConf, createSimpleTopology());
        cluster.waitForIdle();

        // Verify initial state
        assertEquals(2, adapter.getNodeCount(cluster, "supervisor"));
        HealthCheckResult initialHealth = adapter.getHealthCheck().checkHealth(cluster);
        assertTrue(initialHealth.isPassed(), "Cluster should be healthy initially");

        // Capture state before restart
        ClusterState beforeState = adapter.getStateCapture().captureState(cluster);

        // Restart first supervisor with graceful mode
        adapter.restartNode(cluster, "supervisor", 0, RestartMode.GRACEFUL);

        // Verify cluster is still healthy
        HealthCheckResult afterHealth = adapter.getHealthCheck().checkHealth(cluster);
        assertTrue(afterHealth.isPassed(), "Cluster should be healthy after restart");

        // Verify state consistency
        ClusterState afterState = adapter.getStateCapture().captureState(cluster);
        adapter.getStateCapture().verifyState(cluster, beforeState, afterState);

        // Verify supervisor count unchanged
        assertEquals(2, adapter.getNodeCount(cluster, "supervisor"));
    }

    @Test
    public void testSupervisorCrashRestart() throws Exception {
        cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withSupervisors(1)
            .build();

        // Submit test topology
        cluster.submitTopology("crash-test", new HashMap<>(), createSimpleTopology());
        cluster.waitForIdle();

        // Restart with crash mode
        adapter.restartNode(cluster, "supervisor", 0, RestartMode.CRASH);

        // Verify cluster recovered
        HealthCheckResult health = adapter.getHealthCheck().checkHealth(cluster);
        assertTrue(health.isPassed(), "Cluster should recover after crash");
    }

    @Test
    public void testSupervisorDelayedCrashRestart() throws Exception {
        cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withSupervisors(2)
            .build();

        // Submit test topology
        cluster.submitTopology("delayed-test", new HashMap<>(), createSimpleTopology());
        cluster.waitForIdle();

        // Restart with delayed crash mode
        adapter.restartNode(cluster, "supervisor", 1, RestartMode.DELAYED_CRASH);

        // Verify cluster recovered
        HealthCheckResult health = adapter.getHealthCheck().checkHealth(cluster);
        assertTrue(health.isPassed(), "Cluster should recover after delayed crash");
    }

    @Test
    public void testRestartAllSupervisors() throws Exception {
        cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withSupervisors(2)
            .build();

        // Submit test topology
        cluster.submitTopology("all-supervisors-test", new HashMap<>(),
            createSimpleTopology());
        cluster.waitForIdle();

        // Restart all supervisors
        adapter.restartAllNodes(cluster, "supervisor", RestartMode.GRACEFUL);

        // Verify cluster recovered
        HealthCheckResult health = adapter.getHealthCheck().checkHealth(cluster);
        assertTrue(health.isPassed(),
            "Cluster should recover after restarting all supervisors");

        assertEquals(2, adapter.getNodeCount(cluster, "supervisor"));
    }

    @Test
    public void testNimbusRestartThrowsException() throws Exception {
        cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .build();

        // Attempting to restart Nimbus should throw UnsupportedOperationException
        assertThrows(UnsupportedOperationException.class, () -> {
            adapter.restartNode(cluster, "nimbus", 0, RestartMode.GRACEFUL);
        });
    }

    @Test
    public void testMasterRoleThrowsException() throws Exception {
        cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .build();

        // "master" should be normalized to "nimbus" and throw exception
        assertThrows(UnsupportedOperationException.class, () -> {
            adapter.restartNode(cluster, "master", 0, RestartMode.GRACEFUL);
        });
    }

    @Test
    public void testInvalidRoleThrowsException() throws Exception {
        cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .build();

        assertThrows(IllegalArgumentException.class, () -> {
            adapter.restartNode(cluster, "invalid-role", 0, RestartMode.GRACEFUL);
        });
    }

    @Test
    public void testInvalidSupervisorIndexThrowsException() throws Exception {
        cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withSupervisors(1)
            .build();

        // Index 5 is out of range (only 1 supervisor)
        assertThrows(IllegalArgumentException.class, () -> {
            adapter.restartNode(cluster, "supervisor", 5, RestartMode.GRACEFUL);
        });
    }

    @Test
    public void testGetNodeCount() throws Exception {
        cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withSupervisors(3)
            .build();

        assertEquals(3, adapter.getNodeCount(cluster, "supervisor"));
        assertEquals(1, adapter.getNodeCount(cluster, "nimbus"));
    }

    @Test
    public void testWaitActive() throws Exception {
        cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .build();

        // Should complete without exception
        adapter.waitActive(cluster);
    }

    @Test
    public void testHealthCheckDetectsIssues() throws Exception {
        cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .build();

        // Submit topology but don't wait for it to start
        cluster.submitTopology("health-test", new HashMap<>(), createSimpleTopology());

        // Health check should still pass (topology may still be starting)
        HealthCheckResult health = adapter.getHealthCheck().checkHealth(cluster);

        // Just verify the check runs without exception
        assertNotNull(health);
        assertNotNull(health.getMetrics());
    }

    @Test
    public void testStateCaptureAndVerify() throws Exception {
        cluster = new LocalCluster.Builder()
            .withSimulatedTime()
            .withSupervisors(2)
            .build();

        cluster.submitTopology("state-test", new HashMap<>(), createSimpleTopology());
        cluster.waitForIdle();

        // Capture state
        ClusterState state1 = adapter.getStateCapture().captureState(cluster);
        assertNotNull(state1);
        assertNotNull(state1.getStateMap());

        // Capture again - should be consistent
        ClusterState state2 = adapter.getStateCapture().captureState(cluster);

        // Verify states match
        adapter.getStateCapture().verifyState(cluster, state1, state2);
    }

    /**
     * Create a simple test topology with a spout and bolt.
     */
    private StormTopology createSimpleTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("test-spout", new TestWordSpout(), 1);
        builder.setBolt("test-bolt", new TestWordCounter(), 2)
            .shuffleGrouping("test-spout");

        return builder.createTopology();
    }
}
