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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.storm.LocalCluster;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.generated.TopologySummary;
import org.restarttest.state.AbstractStateCapture;
import org.restarttest.state.ClusterState;
import org.restarttest.state.DefaultClusterState;
import org.restarttest.state.StateVerificationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State capture and verification for Storm LocalCluster.
 * <p>
 * Captures basic cluster state including:
 * - Topology count, names, and statuses
 * - Supervisor count and IDs
 * - Worker/executor count per topology
 * <p>
 * Note: This implementation focuses on cluster-level state verification
 * rather than detailed metrics, as worker ports may change after restart.
 */
public class StormStateCapture extends AbstractStateCapture<LocalCluster> {
    private static final Logger LOG = LoggerFactory.getLogger(StormStateCapture.class);

    @Override
    public ClusterState captureState(LocalCluster cluster) throws Exception {
        LOG.debug("Capturing Storm cluster state");

        Map<String, Object> state = new HashMap<>();
        ClusterSummary summary = cluster.getClusterInfo();

        // Capture topology states
        Map<String, Object> topologies = new HashMap<>();
        for (TopologySummary topo : summary.get_topologies()) {
            Map<String, Object> topoState = new HashMap<>();
            topoState.put("name", topo.get_name());
            topoState.put("status", topo.get_status());
            topoState.put("num_executors", topo.get_num_executors());
            topoState.put("num_workers", topo.get_num_workers());
            topoState.put("num_tasks", topo.get_num_tasks());

            topologies.put(topo.get_id(), topoState);

            LOG.debug("Captured topology: {} (status={}, executors={}, workers={})",
                topo.get_name(), topo.get_status(),
                topo.get_num_executors(), topo.get_num_workers());
        }
        state.put("topologies", topologies);
        state.put("topology_count", topologies.size());

        // Capture supervisor states
        state.put("supervisor_count", summary.get_supervisors_size());

        List<String> supervisorIds = summary.get_supervisors().stream()
            .map(SupervisorSummary::get_supervisor_id)
            .collect(Collectors.toList());
        state.put("supervisor_ids", supervisorIds);

        LOG.debug("Captured cluster state: {} topologies, {} supervisors",
            topologies.size(), supervisorIds.size());

        return new DefaultClusterState(state);
    }

    @Override
    protected void verifyCustomInvariants(LocalCluster cluster,
                                         ClusterState before,
                                         ClusterState after) throws Exception {
        LOG.debug("Verifying Storm cluster state invariants");

        Map<String, Object> beforeState = before.getStateMap();
        Map<String, Object> afterState = after.getStateMap();

        // Verify topology count unchanged
        @SuppressWarnings("unchecked")
        Map<String, Object> beforeTopos =
            (Map<String, Object>) beforeState.get("topologies");
        @SuppressWarnings("unchecked")
        Map<String, Object> afterTopos =
            (Map<String, Object>) afterState.get("topologies");

        if (beforeTopos == null || afterTopos == null) {
            throw new StateVerificationException(
                "Topology state missing: before=" + (beforeTopos != null) +
                ", after=" + (afterTopos != null));
        }

        if (beforeTopos.size() != afterTopos.size()) {
            throw new StateVerificationException(
                "Topology count changed: " + beforeTopos.size() +
                " -> " + afterTopos.size());
        }

        // Verify each topology still exists with same status
        for (String topoId : beforeTopos.keySet()) {
            if (!afterTopos.containsKey(topoId)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> beforeTopo =
                    (Map<String, Object>) beforeTopos.get(topoId);
                throw new StateVerificationException(
                    "Topology lost after restart: " + beforeTopo.get("name") +
                    " (id=" + topoId + ")");
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> beforeTopo =
                (Map<String, Object>) beforeTopos.get(topoId);
            @SuppressWarnings("unchecked")
            Map<String, Object> afterTopo =
                (Map<String, Object>) afterTopos.get(topoId);

            String beforeStatus = (String) beforeTopo.get("status");
            String afterStatus = (String) afterTopo.get("status");

            // Status should remain the same (ACTIVE should stay ACTIVE, etc.)
            if (!beforeStatus.equals(afterStatus)) {
                throw new StateVerificationException(
                    "Topology status changed for " + beforeTopo.get("name") + ": " +
                    beforeStatus + " -> " + afterStatus);
            }

            LOG.debug("Verified topology {}: status={}, executors={}, workers={}",
                beforeTopo.get("name"), afterStatus,
                afterTopo.get("num_executors"), afterTopo.get("num_workers"));
        }

        // Verify supervisor count unchanged
        // Use compareStateValue from AbstractStateCapture
        compareStateValue("supervisor_count", before, after, false);

        LOG.debug("All state invariants verified successfully");
    }
}
