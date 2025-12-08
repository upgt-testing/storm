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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.LocalCluster;
import org.apache.storm.ProcessSimulator;
import org.apache.storm.daemon.supervisor.ReadClusterState;
import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.Time;
import org.restarttest.core.ClusterAdapter;
import org.restarttest.core.RestartMode;
import org.restarttest.health.HealthCheck;
import org.restarttest.state.StateCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Restart testing adapter for Apache Storm LocalCluster.
 * <p>
 * Supports restarting:
 * - Supervisors (full support for all restart modes)
 * - Workers (full support for all restart modes)
 * <p>
 * Does NOT support:
 * - Nimbus restart (architectural limitation of LocalCluster)
 */
public class StormClusterAdapter implements ClusterAdapter<LocalCluster> {
    private static final Logger LOG = LoggerFactory.getLogger(StormClusterAdapter.class);

    private final StormStateCapture stateCapture;
    private final StormHealthCheck healthCheck;

    public StormClusterAdapter() {
        this.stateCapture = new StormStateCapture();
        this.healthCheck = new StormHealthCheck();
    }

    @Override
    public Class<LocalCluster> getClusterType() {
        return LocalCluster.class;
    }

    @Override
    public void restartNode(LocalCluster cluster, String nodeRole, int nodeIndex,
                           RestartMode mode) throws Exception {
        String role = normalizeRole(nodeRole);

        LOG.info("Restarting {} node at index {} with mode {}", role, nodeIndex, mode);

        if ("nimbus".equals(role)) {
            throw new UnsupportedOperationException(
                "Nimbus restart is not supported in LocalCluster. " +
                "Nimbus is created during cluster construction and cannot be restarted. " +
                "To test Nimbus failures, close and recreate the entire LocalCluster.");
        } else if ("supervisor".equals(role)) {
            restartSupervisor(cluster, nodeIndex, mode);
        } else if ("worker".equals(role)) {
            restartWorker(cluster, nodeIndex, mode);
        } else {
            throw new IllegalArgumentException(
                "Unknown node role: " + nodeRole +
                ". Supported roles: supervisor, worker");
        }
    }

    @Override
    public void restartAllNodes(LocalCluster cluster, String nodeRole, RestartMode mode)
            throws Exception {
        String role = normalizeRole(nodeRole);
        int count = getNodeCount(cluster, role);

        LOG.info("Restarting all {} nodes ({} total) with mode {}", role, count, mode);

        for (int i = 0; i < count; i++) {
            restartNode(cluster, role, i, mode);
        }
    }

    @Override
    public void waitActive(LocalCluster cluster) throws Exception {
        LOG.debug("Waiting for cluster to become active");
        cluster.waitForIdle();
    }

    @Override
    public StateCapture<LocalCluster> getStateCapture() {
        return stateCapture;
    }

    @Override
    public HealthCheck<LocalCluster> getHealthCheck() {
        return healthCheck;
    }

    @Override
    public int getNodeCount(LocalCluster cluster, String nodeRole) throws Exception {
        String role = normalizeRole(nodeRole);

        if ("supervisor".equals(role)) {
            ClusterSummary summary = cluster.getClusterInfo();
            return summary.get_supervisors_size();
        } else if ("worker".equals(role)) {
            return getWorkerIds(cluster).size();
        } else if ("nimbus".equals(role)) {
            return 1; // Always one Nimbus in LocalCluster
        }

        throw new IllegalArgumentException("Unknown role: " + nodeRole);
    }

    /**
     * Restart a supervisor with the specified mode.
     */
    private void restartSupervisor(LocalCluster cluster, int index, RestartMode mode)
            throws Exception {
        // Get supervisor by index
        List<String> supervisorIds = getSupervisorIds(cluster);

        if (index < 0 || index >= supervisorIds.size()) {
            throw new IllegalArgumentException(
                "Supervisor index " + index + " out of range. " +
                "Available supervisors: " + supervisorIds.size());
        }

        String supervisorId = supervisorIds.get(index);
        Supervisor supervisor = cluster.getSupervisor(supervisorId);

        if (supervisor == null) {
            throw new IllegalStateException("Supervisor not found: " + supervisorId);
        }

        LOG.info("Restarting supervisor {} (index {}) with mode {}", supervisorId, index, mode);

        // Capture configuration before restart
        Map<String, Object> conf = supervisor.getConf();
        @SuppressWarnings("unchecked")
        List<Integer> ports = (List<Integer>) conf.get(DaemonConfig.SUPERVISOR_SLOTS_PORTS);
        int portCount = ports != null ? ports.size() : 2;

        // Perform restart based on mode
        switch (mode) {
            case GRACEFUL:
                // Gracefully shutdown workers first
                LOG.debug("Gracefully shutting down workers on supervisor {}", supervisorId);
                supervisor.shutdownAllWorkers(null, ReadClusterState.THREAD_DUMP_ON_ERROR);
                cluster.waitForIdle();
                break;

            case CRASH:
                // Immediate kill without cleanup
                LOG.debug("Crash-killing supervisor {} without cleanup", supervisorId);
                break;

            case DELAYED_CRASH:
                // Will add delay after kill
                LOG.debug("Will crash supervisor {} with delayed restart", supervisorId);
                break;

            default:
                throw new IllegalArgumentException("Unknown restart mode: " + mode);
        }

        // Kill the supervisor
        cluster.killSupervisor(supervisorId);
        LOG.debug("Killed supervisor {}", supervisorId);

        // Apply delay for DELAYED_CRASH mode
        if (mode == RestartMode.DELAYED_CRASH) {
            LOG.debug("Delaying restart by 30 seconds (simulated time)");
            Time.advanceTimeSecs(30);
            cluster.waitForIdle();
        }

        // Recreate supervisor with same configuration
        LOG.debug("Recreating supervisor {} with {} ports", supervisorId, portCount);
        cluster.addSupervisor(portCount, null, supervisorId);
        cluster.waitForIdle();

        LOG.info("Successfully restarted supervisor {}", supervisorId);
    }

    /**
     * Restart a worker with the specified mode.
     */
    private void restartWorker(LocalCluster cluster, int index, RestartMode mode)
            throws Exception {
        // Find worker by index across all topologies
        List<String> workerIds = getWorkerIds(cluster);

        if (index < 0 || index >= workerIds.size()) {
            throw new IllegalArgumentException(
                "Worker index " + index + " out of range. " +
                "Available workers: " + workerIds.size());
        }

        String workerId = workerIds.get(index);

        LOG.info("Restarting worker {} (index {}) with mode {}", workerId, index, mode);

        // Kill worker via ProcessSimulator (used in local mode)
        // In local mode, there's no distinction between graceful and crash
        // as workers are simulated processes
        LOG.debug("Killing worker process {}", workerId);
        ProcessSimulator.killProcess(workerId);

        // Apply delay for DELAYED_CRASH mode
        if (mode == RestartMode.DELAYED_CRASH) {
            LOG.debug("Delaying restart by 30 seconds (simulated time)");
            Time.advanceTimeSecs(30);
        }

        // Wait for supervisor to detect and restart the worker
        cluster.waitForIdle();

        LOG.info("Worker {} restart completed (supervisor will reassign)", workerId);
    }

    /**
     * Normalize role names to canonical forms.
     * Maps generic names (master/worker) to Storm-specific names.
     */
    private String normalizeRole(String role) {
        String lower = role.toLowerCase();

        // Map generic names to Storm-specific names
        if ("master".equals(lower)) {
            return "nimbus";
        }

        return lower;
    }

    /**
     * Get list of supervisor IDs in the cluster.
     */
    private List<String> getSupervisorIds(LocalCluster cluster) throws TException {
        ClusterSummary summary = cluster.getClusterInfo();
        return summary.get_supervisors().stream()
            .map(SupervisorSummary::get_supervisor_id)
            .collect(Collectors.toList());
    }

    /**
     * Get list of unique worker IDs across all topologies.
     * Worker IDs are in the format "host:port".
     */
    private List<String> getWorkerIds(LocalCluster cluster) throws TException {
        List<String> workerIds = new ArrayList<>();
        ClusterSummary summary = cluster.getClusterInfo();

        for (TopologySummary topo : summary.get_topologies()) {
            TopologyInfo info = cluster.getTopologyInfo(topo.get_id());

            for (ExecutorSummary exec : info.get_executors()) {
                String workerId = exec.get_host() + ":" + exec.get_port();

                // Add unique worker IDs only
                if (!workerIds.contains(workerId)) {
                    workerIds.add(workerId);
                }
            }
        }

        return workerIds;
    }
}
