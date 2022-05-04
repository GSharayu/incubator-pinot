/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core.assignment.segment;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Segment.AssignmentStrategy;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment assignment for offline table.
 * <ul>
 *   <li>
 *     Non-replica-group based assignment (only 1 replica-group in instance partitions):
 *     <p>Assign the segment to the instance with the least number of segments. In case of a tie, assign the segment to
 *     the instance with the smallest index in the list. Use Helix AutoRebalanceStrategy to rebalance the table.
 *   </li>
 *   <li>
 *     Replica-group based assignment (more than 1 replica-groups in instance partitions):
 *     <p>Among replica-groups, always mirror the assignment (pick the same index of the instance).
 *     <p>Within each partition, assign the segment to the instances with the least segments already assigned. In case
 *     of a tie, assign to the instance with the smallest index in the list. Do this for one replica-group and mirror
 *     the assignment to other replica-groups.
 *     <p>To rebalance a table, within each partition, first calculate the number of segments on each instance, loop
 *     over all the segments and keep the assignment if number of segments for the instance has not been reached and
 *     track the not assigned segments, then assign the left-over segments to the instances with the least segments, or
 *     the smallest index if there is a tie. Repeat the process for all the partitions in one replica-group, and mirror
 *     the assignment to other replica-groups. With this greedy algorithm, the result is deterministic and with minimum
 *     segment moves.
 *   </li>
 * </ul>
 */
public class OfflineSegmentAssignment implements SegmentAssignment {
  private static final Logger LOGGER = LoggerFactory.getLogger(OfflineSegmentAssignment.class);

  private HelixManager _helixManager;
  private TableConfig _tableConfig;
  private String _offlineTableName;
  private int _replication;
  private String _partitionColumn;
  SegmentAssignmentStrategy _segmentAssignmentStrategy;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _tableConfig = tableConfig;
    _offlineTableName = tableConfig.getTableName();
    _replication = tableConfig.getValidationConfig().getReplicationNumber();
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    _partitionColumn = replicaGroupStrategyConfig != null ? replicaGroupStrategyConfig.getPartitionColumn() : null;
    if (_partitionColumn == null) {
      LOGGER.info("Initialized OfflineSegmentAssignment with replication: {} without partition column for table: {} ",
          _replication, _offlineTableName);
    } else {
      LOGGER.info("Initialized OfflineSegmentAssignment with replication: {} and partition column: {} for table: {}",
          _replication, _partitionColumn, _offlineTableName);
    }
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    InstancePartitions instancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    Preconditions.checkState(instancePartitions != null, "Failed to find OFFLINE instance partitions for table: %s",
        _offlineTableName);
    LOGGER.info("Assigning segment: {} with instance partitions: {} for table: {}", segmentName, instancePartitions,
        _offlineTableName);
    checkReplication(instancePartitions);

    List<String> instancesAssigned = assignSegment(segmentName, currentAssignment, instancePartitions);

    LOGGER
        .info("Assigned segment: {} to instances: {} for table: {}", segmentName, instancesAssigned, _offlineTableName);
    return instancesAssigned;
  }

  /**
   * Helper method to check whether the number of replica-groups matches the table replication for replica-group based
   * instance partitions. Log a warning if they do not match and use the one inside the instance partitions. The
   * mismatch can happen when table is not configured correctly (table replication and numReplicaGroups does not match
   * or replication changed without reassigning instances).
   */
  private void checkReplication(InstancePartitions instancePartitions) {
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    if (numReplicaGroups != 1 && numReplicaGroups != _replication) {
      LOGGER.warn(
          "Number of replica-groups in instance partitions {}: {} does not match replication in table config: {} for "
              + "table: {}, use: {}", instancePartitions.getInstancePartitionsName(), numReplicaGroups, _replication,
          _offlineTableName, numReplicaGroups);
    }
  }

  /**
   * Helper method to assign instances based on the current assignment and instance partitions.
   */
  private List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions) {
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    // Determine segment assignment strategy for assigning segment
    getSegmentAssignmentStrategy(_tableConfig.getSegmentAssignmentConfigMap().
        get(InstancePartitionsType.OFFLINE).getAssignmentStrategy(), numReplicaGroups);
    return _segmentAssignmentStrategy.assignSegment(segmentName, currentAssignment, instancePartitions, InstancePartitionsType.OFFLINE);
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, Configuration config) {

    InstancePartitions offlineInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    int numReplicaGroups = offlineInstancePartitions.getNumReplicaGroups();
    // Determine segment assignment strategy for reassigning segment
    getSegmentAssignmentStrategy(_tableConfig.getSegmentAssignmentConfigMap().
        get(InstancePartitionsType.OFFLINE).getAssignmentStrategy(), numReplicaGroups);

    // Separate Dim table flow for table rebalance
    if (_segmentAssignmentStrategy instanceof OfflineAllServersSegmentAssignmentStrategy) {
      return _segmentAssignmentStrategy.reassignSegments(currentAssignment, offlineInstancePartitions, InstancePartitionsType.OFFLINE);
    } else {
      // Keep the tier storage logic in segment assignment
      Preconditions
          .checkState(offlineInstancePartitions != null, "Failed to find OFFLINE instance partitions for table: %s",
              _offlineTableName);
      boolean bootstrap =
          config.getBoolean(RebalanceConfigConstants.BOOTSTRAP, RebalanceConfigConstants.DEFAULT_BOOTSTRAP);

      Map<String, Map<String, String>> nonTierAssignment = currentAssignment;
      // Rebalance tiers first
      List<Map<String, Map<String, String>>> newTierAssignments = null;
      if (sortedTiers != null) {
        Preconditions.checkState(tierInstancePartitionsMap != null, "Tier to instancePartitions map is null");
        LOGGER.info("Rebalancing tiers: {} for table: {} with bootstrap: {}", tierInstancePartitionsMap.keySet(),
            _offlineTableName, bootstrap);

        // Get tier to segment assignment map i.e. current assignments split by tiers they are eligible for
        SegmentAssignmentUtils.TierSegmentAssignment tierSegmentAssignment =
            new SegmentAssignmentUtils.TierSegmentAssignment(_offlineTableName, sortedTiers, currentAssignment);
        Map<String, Map<String, Map<String, String>>> tierNameToSegmentAssignmentMap =
            tierSegmentAssignment.getTierNameToSegmentAssignmentMap();

        // For each tier, calculate new assignment using instancePartitions for that tier
        newTierAssignments = new ArrayList<>(tierNameToSegmentAssignmentMap.size());
        for (Map.Entry<String, Map<String, Map<String, String>>> entry : tierNameToSegmentAssignmentMap.entrySet()) {
          String tierName = entry.getKey();
          Map<String, Map<String, String>> tierCurrentAssignment = entry.getValue();

          InstancePartitions tierInstancePartitions = tierInstancePartitionsMap.get(tierName);
          Preconditions
              .checkNotNull(tierInstancePartitions, "Failed to find instance partitions for tier: %s of table: %s",
                  tierName, _offlineTableName);
          checkReplication(tierInstancePartitions);

          LOGGER.info("Rebalancing tier: {} for table: {} with bootstrap: {}, instance partitions: {}", tierName,
              _offlineTableName, bootstrap, tierInstancePartitions);
          newTierAssignments.add(reassignSegments(tierName, tierCurrentAssignment, tierInstancePartitions, bootstrap));
        }

        // Rest of the operations should happen only on segments which were not already assigned as part of tiers
        nonTierAssignment = tierSegmentAssignment.getNonTierSegmentAssignment();
      }

      LOGGER.info("Rebalancing table: {} with instance partitions: {}, bootstrap: {}", _offlineTableName,
          offlineInstancePartitions, bootstrap);
      checkReplication(offlineInstancePartitions);
      Map<String, Map<String, String>> newAssignment =
          reassignSegments(InstancePartitionsType.OFFLINE.toString(), nonTierAssignment, offlineInstancePartitions,
              bootstrap);

      // add tier assignments, if available
      if (CollectionUtils.isNotEmpty(newTierAssignments)) {
        newTierAssignments.forEach(newAssignment::putAll);
      }

      LOGGER.info("Rebalanced table: {}, number of segments to be moved to each instance: {}", _offlineTableName,
          SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment));
      return newAssignment;
    }
  }

  /**
   * Rebalances segments in the current assignment using the instancePartitions and returns new assignment
   */
  private Map<String, Map<String, String>> reassignSegments(String instancePartitionType,
      Map<String, Map<String, String>> currentAssignment, InstancePartitions instancePartitions, boolean bootstrap) {
    Map<String, Map<String, String>> newAssignment;
    if (bootstrap) {
      LOGGER.info("Bootstrapping segment assignment for {} segments of table: {}", instancePartitionType,
          _offlineTableName);

      // When bootstrap is enabled, start with an empty assignment and reassign all segments
      newAssignment = new TreeMap<>();
      for (String segment : currentAssignment.keySet()) {
        List<String> assignedInstances = assignSegment(segment, newAssignment, instancePartitions);
        newAssignment
            .put(segment, SegmentAssignmentUtils.getInstanceStateMap(assignedInstances, SegmentStateModel.ONLINE));
      }
    } else {
      newAssignment = _segmentAssignmentStrategy.reassignSegments(currentAssignment, instancePartitions, InstancePartitionsType.OFFLINE);
    }
    return newAssignment;
  }

  private void getSegmentAssignmentStrategy(AssignmentStrategy assignmentStrategy, int numReplicaGroups) {
    String assignmentStrategyName = assignmentStrategy.toString();
    switch(assignmentStrategyName) {
      case AssignmentStrategy.ALL_SERVERS_SEGMENT_ASSIGNMENT_STRATEGY:
        Preconditions.checkState(_tableConfig.isDimTable(), "Not a dimension table: %s" + _offlineTableName);
        _segmentAssignmentStrategy = new OfflineAllServersSegmentAssignmentStrategy();
        break;
      case AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY:
        Preconditions.checkState(numReplicaGroups != 1, "Not more than 1 replica group: %s" + _offlineTableName);
        _segmentAssignmentStrategy = new OfflineReplicaGroupSegmentAssignmentStrategy();
        break;
      case AssignmentStrategy.BALANCE_NUM_SEGMENT_ASSIGNMENT_STRATEGY:
      default:
        _segmentAssignmentStrategy = new OfflineBalanceNumSegmentAssignmentStrategy();
    }
    _segmentAssignmentStrategy.init(_helixManager, _tableConfig);
  }
}
