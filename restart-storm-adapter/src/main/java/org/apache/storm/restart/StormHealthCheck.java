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

import org.apache.storm.LocalCluster;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.TopologySummary;
import org.restarttest.health.HealthCheck;
import org.restarttest.health.HealthCheckResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Health check implementation for Storm LocalCluster.
 * <p>
 * Verifies:
 * - Cluster is responsive (can get cluster info)
 * - Nimbus has elected a leader
 * - Active topologies have executors assigned
 * - Cluster reaches idle state
 */
public class StormHealthCheck implements HealthCheck<LocalCluster> {
    private static final Logger LOG = LoggerFactory.getLogger(StormHealthCheck.class);

    /**
     * Timeout for waiting for cluster to become idle (milliseconds).
     */
    private static final long IDLE_TIMEOUT_MS = 5000;

    @Override
    public HealthCheckResult checkHealth(LocalCluster cluster) throws Exception {
        HealthCheckResult result = new HealthCheckResult(true, getName());

        LOG.debug("Starting Storm cluster health check");

        try {
            // Check 1: Cluster is responsive
            ClusterSummary summary;
            try {
                summary = cluster.getClusterInfo();
            } catch (Exception e) {
                result.addFailure("Cluster not responsive: " + e.getMessage());
                LOG.error("Cluster not responsive", e);
                return result;
            }

            // Add metrics
            int supervisorCount = summary.get_supervisors_size();
            int topologyCount = summary.get_topologies_size();

            result.addMetric("supervisor_count", supervisorCount);
            result.addMetric("topology_count", topologyCount);

            LOG.debug("Cluster responsive: {} supervisors, {} topologies",
                supervisorCount, topologyCount);

            // Check 2: Nimbus has leader
            boolean hasLeader = false;
            if (summary.is_set_nimbuses()) {
                for (NimbusSummary nimbus : summary.get_nimbuses()) {
                    if (nimbus.is_isLeader()) {
                        hasLeader = true;
                        result.addMetric("nimbus_leader", nimbus.get_host());
                        LOG.debug("Nimbus leader found: {}", nimbus.get_host());
                        break;
                    }
                }
            }

            if (!hasLeader) {
                result.addFailure("No Nimbus leader elected");
                LOG.warn("No Nimbus leader elected");
            }

            // Check 3: All active topologies have executors assigned
            int activeTopologies = 0;
            int topologiesWithoutExecutors = 0;

            for (TopologySummary topo : summary.get_topologies()) {
                String status = topo.get_status();

                if ("ACTIVE".equals(status)) {
                    activeTopologies++;

                    if (topo.get_num_executors() == 0) {
                        result.addFailure("Active topology has no executors: " +
                            topo.get_name());
                        topologiesWithoutExecutors++;
                        LOG.warn("Active topology {} has no executors", topo.get_name());
                    } else {
                        LOG.debug("Topology {} is healthy: {} executors, {} workers",
                            topo.get_name(), topo.get_num_executors(),
                            topo.get_num_workers());
                    }
                }
            }

            result.addMetric("active_topologies", activeTopologies);
            result.addMetric("topologies_without_executors", topologiesWithoutExecutors);

            // Check 4: Cluster is idle (all components waiting)
            try {
                LOG.debug("Waiting for cluster to become idle (timeout: {}ms)", IDLE_TIMEOUT_MS);
                cluster.waitForIdle(IDLE_TIMEOUT_MS);
                LOG.debug("Cluster is idle");
            } catch (AssertionError e) {
                result.addFailure("Cluster not idle after " + IDLE_TIMEOUT_MS + "ms: " +
                    e.getMessage());
                LOG.warn("Cluster not idle after {}ms", IDLE_TIMEOUT_MS);
            } catch (InterruptedException e) {
                result.addFailure("Interrupted while waiting for idle: " + e.getMessage());
                LOG.warn("Interrupted while waiting for idle", e);
                Thread.currentThread().interrupt();
            }

        } catch (Exception e) {
            result.addFailure("Health check failed with exception: " + e.getMessage());
            LOG.error("Health check failed with exception", e);
        }

        if (result.isPassed()) {
            LOG.info("Storm cluster health check PASSED");
        } else {
            LOG.warn("Storm cluster health check FAILED: {}", result.getFailures());
        }

        return result;
    }

    @Override
    public String getName() {
        return "storm-cluster-health";
    }
}
