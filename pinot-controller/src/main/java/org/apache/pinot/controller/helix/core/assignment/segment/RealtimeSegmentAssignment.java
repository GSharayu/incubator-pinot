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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Segment.AssignmentStrategy;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment assignment for LLC real-time table.
 * <ul>
 *   <li>
 *     For the CONSUMING segments, it is very similar to replica-group based segment assignment with the following
 *     differences:
 *     <ul>
 *       <li>
 *         1. Within a replica-group, all segments of the same stream partition are always assigned to the same exactly
 *         one instance, and because of that we can directly assign or rebalance the CONSUMING segments to the instances
 *         based on the partition id
 *       </li>
 *       <li>
 *         2. Partition id for an instance is derived from the index of the instance (within the replica-group for
 *         replica-group based assignment), instead of explicitly stored in the instance partitions
 *       </li>
 *       <li>
 *         TODO: Support explicit partition configuration in instance partitions
 *       </li>
 *     </ul>
 *   </li>
 *   <li>
 *     For the COMPLETED segments:
 *     <ul>
 *       <li>
 *         If COMPLETED instance partitions are provided, reassign COMPLETED segments the same way as
 *         OfflineSegmentAssignment to relocate COMPLETED segments and offload them from CONSUMING instances to
 *         COMPLETED instances
 *       </li>
 *       <li>
 *         If COMPLETED instance partitions are not provided, reassign COMPLETED segments the same way as CONSUMING
 *         segments with CONSUMING instance partitions to ensure COMPLETED segments are served by the correct instances
 *         when instances for the table has been changed.
 *       </li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class RealtimeSegmentAssignment implements SegmentAssignment {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentAssignment.class);

  private HelixManager _helixManager;
  private TableConfig _tableConfig;
  private String _realtimeTableName;
  private int _replication;
  private String _partitionColumn;
  private SegmentAssignmentStrategy _segmentAssignmentStrategy;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _tableConfig = tableConfig;
    _realtimeTableName = tableConfig.getTableName();
    _replication = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    _partitionColumn = replicaGroupStrategyConfig != null ? replicaGroupStrategyConfig.getPartitionColumn() : null;
    LOGGER.info("Initialized RealtimeSegmentAssignment with replication: {}, partitionColumn: {} for table: {}",
        _replication, _partitionColumn, _realtimeTableName);
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap) {
    Preconditions.checkState(instancePartitionsMap.size() == 1, "One instance partition type should be provided");
    Map.Entry<InstancePartitionsType, InstancePartitions> typeToInstancePartitions =
        instancePartitionsMap.entrySet().iterator().next();
    InstancePartitionsType instancePartitionsType = typeToInstancePartitions.getKey();
    InstancePartitions instancePartitions = typeToInstancePartitions.getValue();
    Preconditions
        .checkState(instancePartitions.getNumPartitions() == 1, "Instance partitions: %s should contain 1 partition",
            instancePartitions.getInstancePartitionsName());
    LOGGER.info("Assigning segment: {} with instance partitions: {} for table: {}", segmentName, instancePartitions,
        _realtimeTableName);
    checkReplication(instancePartitions);
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    List<String> instancesAssigned;
    if (instancePartitionsType == InstancePartitionsType.CONSUMING) {
      getSegmentAssignmentStrategy(_tableConfig.getSegmentAssignmentConfigMap().get(InstancePartitionsType.CONSUMING).getAssignmentStrategy(), numReplicaGroups);
      instancesAssigned = assignConsumingSegment(segmentName, instancePartitions);
    } else {
      getSegmentAssignmentStrategy(_tableConfig.getSegmentAssignmentConfigMap().get(InstancePartitionsType.COMPLETED).getAssignmentStrategy(), numReplicaGroups);
      instancesAssigned = assignCompletedSegment(segmentName, currentAssignment, instancePartitions);
    }

    LOGGER.info("Assigned segment: {} to instances: {} for table: {}", segmentName, instancesAssigned,
        _realtimeTableName);
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
          _realtimeTableName, numReplicaGroups);
    }
  }

  /**
   * Helper method to assign instances for CONSUMING segment based on the segment partition id and instance partitions.
   */
  private List<String> assignConsumingSegment(String segmentName, InstancePartitions instancePartitions) {
    // Setting current assignment for consuming segments as null
    return _segmentAssignmentStrategy.assignSegment(segmentName, null, instancePartitions, InstancePartitionsType.CONSUMING);
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, Configuration config) {

    InstancePartitions completedInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.COMPLETED);
    InstancePartitions consumingInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    Preconditions
        .checkState(consumingInstancePartitions != null, "Failed to find CONSUMING instance partitions for table: %s",
            _realtimeTableName);
    Preconditions.checkState(consumingInstancePartitions.getNumPartitions() == 1,
        "Instance partitions: %s should contain 1 partition", consumingInstancePartitions.getInstancePartitionsName());
    boolean includeConsuming = config
        .getBoolean(RebalanceConfigConstants.INCLUDE_CONSUMING, RebalanceConfigConstants.DEFAULT_INCLUDE_CONSUMING);
    boolean bootstrap =
        config.getBoolean(RebalanceConfigConstants.BOOTSTRAP, RebalanceConfigConstants.DEFAULT_BOOTSTRAP);

    // Rebalance tiers first
    Map<String, Map<String, String>> nonTierAssignment = currentAssignment;
    List<Map<String, Map<String, String>>> newTierAssignments = null;
    if (sortedTiers != null) {
      Preconditions.checkState(tierInstancePartitionsMap != null, "Tier to instancePartitions map is null");
      LOGGER.info("Rebalancing tiers: {} for table: {} with bootstrap: {}", tierInstancePartitionsMap.keySet(),
          _realtimeTableName, bootstrap);

      // Get tier to segment assignment map for ONLINE segments i.e. current assignments split by tiers they are
      // eligible for
      SegmentAssignmentUtils.TierSegmentAssignment tierSegmentAssignment =
          new SegmentAssignmentUtils.TierSegmentAssignment(_realtimeTableName, sortedTiers, currentAssignment);
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
                tierName, _realtimeTableName);
        checkReplication(tierInstancePartitions);

        LOGGER.info("Rebalancing tier: {} for table: {} with bootstrap: {}, instance partitions: {}", tierName,
            _realtimeTableName, bootstrap, tierInstancePartitions);
        newTierAssignments.add(reassignSegments(tierName, tierCurrentAssignment, tierInstancePartitions, bootstrap));
      }

      // Rest of the operations should happen only on segments which were not already assigned as part of tiers
      nonTierAssignment = tierSegmentAssignment.getNonTierSegmentAssignment();
    }

    LOGGER.info("Rebalancing table: {} with COMPLETED instance partitions: {}, CONSUMING instance partitions: {}, "
            + "includeConsuming: {}, bootstrap: {}", _realtimeTableName, completedInstancePartitions,
        consumingInstancePartitions, includeConsuming, bootstrap);
    if (completedInstancePartitions != null) {
      checkReplication(completedInstancePartitions);
    }
    checkReplication(consumingInstancePartitions);

    SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment completedConsumingOfflineSegmentAssignment =
        new SegmentAssignmentUtils.CompletedConsumingOfflineSegmentAssignment(nonTierAssignment);
    Map<String, Map<String, String>> newAssignment;

    // Reassign COMPLETED segments first
    Map<String, Map<String, String>> completedSegmentAssignment =
        completedConsumingOfflineSegmentAssignment.getCompletedSegmentAssignment();
    if (completedInstancePartitions != null) {
      // When COMPLETED instance partitions are provided, reassign COMPLETED segments in a balanced way (relocate
      // COMPLETED segments to offload them from CONSUMING instances to COMPLETED instances)
      LOGGER
          .info("Reassigning COMPLETED segments with COMPLETED instance partitions for table: {}", _realtimeTableName);
      newAssignment = reassignSegments(InstancePartitionsType.COMPLETED.toString(), completedSegmentAssignment,
          completedInstancePartitions, bootstrap);
    } else {
      // When COMPLETED instance partitions are not provided, reassign COMPLETED segments the same way as CONSUMING
      // segments with CONSUMING instance partitions (ensure COMPLETED segments are served by the correct instances when
      // instances for the table has been changed)
      LOGGER.info(
          "No COMPLETED instance partitions found, reassigning COMPLETED segments the same way as CONSUMING segments "
              + "with CONSUMING instance partitions for table: {}", _realtimeTableName);

      newAssignment = new TreeMap<>();
      for (String segmentName : completedSegmentAssignment.keySet()) {
        List<String> instancesAssigned = assignConsumingSegment(segmentName, consumingInstancePartitions);
        Map<String, String> instanceStateMap =
            SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE);
        newAssignment.put(segmentName, instanceStateMap);
      }
    }

    // Reassign CONSUMING segments if configured
    Map<String, Map<String, String>> consumingSegmentAssignment =
        completedConsumingOfflineSegmentAssignment.getConsumingSegmentAssignment();
    if (includeConsuming) {
      LOGGER
          .info("Reassigning CONSUMING segments with CONSUMING instance partitions for table: {}", _realtimeTableName);

      for (String segmentName : consumingSegmentAssignment.keySet()) {
        List<String> instancesAssigned = assignConsumingSegment(segmentName, consumingInstancePartitions);
        Map<String, String> instanceStateMap =
            SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.CONSUMING);
        newAssignment.put(segmentName, instanceStateMap);
      }
    } else {
      newAssignment.putAll(consumingSegmentAssignment);
    }

    // Keep the OFFLINE segments not moved, and RealtimeSegmentValidationManager will periodically detect the OFFLINE
    // segments and re-assign them
    newAssignment.putAll(completedConsumingOfflineSegmentAssignment.getOfflineSegmentAssignment());

    // Add tier assignments, if available
    if (CollectionUtils.isNotEmpty(newTierAssignments)) {
      newTierAssignments.forEach(newAssignment::putAll);
    }

    LOGGER.info("Rebalanced table: {}, number of segments to be moved to each instance: {}", _realtimeTableName,
        SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment));
    return newAssignment;
  }

  /**
   * Rebalances segments in the current assignment using the instancePartitions and returns new assignment
   */
  private Map<String, Map<String, String>> reassignSegments(String instancePartitionType,
      Map<String, Map<String, String>> currentAssignment, InstancePartitions instancePartitions, boolean bootstrap) {
    Map<String, Map<String, String>> newAssignment;
    if (bootstrap) {
      LOGGER.info("Bootstrapping segment assignment for {} segments of table: {}", instancePartitionType,
          _realtimeTableName);

      // When bootstrap is enabled, start with an empty assignment and reassign all segments
      newAssignment = new TreeMap<>();
      for (String segment : currentAssignment.keySet()) {
        List<String> assignedInstances = assignCompletedSegment(segment, newAssignment, instancePartitions);
        newAssignment
            .put(segment, SegmentAssignmentUtils.getInstanceStateMap(assignedInstances, SegmentStateModel.ONLINE));
      }
    } else {
      return _segmentAssignmentStrategy.reassignSegments(currentAssignment, instancePartitions, null);
    }
    return newAssignment;
  }

  /**
   * Helper method to assign instances for COMPLETED segment based on the current assignment and instance partitions.
   */
  private List<String> assignCompletedSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions) {
    return _segmentAssignmentStrategy.assignSegment(segmentName, currentAssignment, instancePartitions, InstancePartitionsType.COMPLETED);
  }

  private int getPartitionGroupId(String segmentName) {
    Integer segmentPartitionId =
        SegmentUtils.getRealtimeSegmentPartitionId(segmentName, _realtimeTableName, _helixManager, _partitionColumn);
    if (segmentPartitionId == null) {
      // This case is for the uploaded segments for which there's no partition information.
      // A random, but consistent, partition id is calculated based on the hash code of the segment name.
      // Note that '% 10K' is used to prevent having partition ids with large value which will be problematic later in
      // instance assignment formula.
      segmentPartitionId = Math.abs(segmentName.hashCode() % 10_000);
    }
    return segmentPartitionId;
  }

  private void getSegmentAssignmentStrategy(AssignmentStrategy assignmentStrategy, int numReplicaGroups) {
    String assignmentStrategyName = assignmentStrategy.toString();
    switch (assignmentStrategyName) {
      case AssignmentStrategy.BALANCE_NUM_SEGMENT_ASSIGNMENT_STRATEGY:
        _segmentAssignmentStrategy = new RealtimeBalanceNumSegmentAssignmentStrategy();
        break;
      case AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY:
      case AssignmentStrategy.PARTITION_BASED_SEGMENT_ASSIGNMENT_STRATEGY:
      default:
        _segmentAssignmentStrategy = new RealtimeReplicaGroupPartitionBasedSegmentAssignmentStrategy();
    }
    _segmentAssignmentStrategy.init(_helixManager, _tableConfig);
  }
}
