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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RealtimeReplicaGroupPartitionBasedSegmentAssignmentStrategy implements SegmentAssignmentStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeBalanceNumSegmentAssignmentStrategy.class);

  private static HelixManager _helixManager;
  private String _tableName;
  private int _replication;
  private String _partitionColumn;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _tableName = tableConfig.getTableName();
    _replication = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    _partitionColumn = replicaGroupStrategyConfig != null ? replicaGroupStrategyConfig.getPartitionColumn() : null;
    LOGGER.info("Initialized RealtimeSegmentAssignment with replication: {}, partitionColumn: {} for table: {}",
        _replication, _partitionColumn, _tableName);
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, @Nullable InstancePartitionsType instancePartitionsType) {
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    if (instancePartitionsType == InstancePartitionsType.COMPLETED) {
      // Uniformly spray the segment partitions over the instance partitions
      int partitionId = getPartitionGroupId(segmentName) % instancePartitions.getNumPartitions();
      return SegmentAssignmentUtils.assignSegmentWithReplicaGroup(currentAssignment, instancePartitions, partitionId);
    } else {
      // TODO Preconditions to check if its CONSUMING
      // Replica-group based assignment:
      // Within a replica-group, uniformly spray the partitions across the instances.
      // E.g. (within a replica-group, 3 instances, 6 partitions)
      // "0_0": [i0, i1, i2]
      //         p0  p1  p2
      //         p3  p4  p5
      int partitionGroupId = getPartitionGroupId(segmentName);
      List<String> instancesAssigned = new ArrayList<>(numReplicaGroups);
      for (int replicaGroupId = 0; replicaGroupId < numReplicaGroups; replicaGroupId++) {
        List<String> instances = instancePartitions.getInstances(0, replicaGroupId);
        instancesAssigned.add(instances.get(partitionGroupId % instances.size()));
      }
      return instancesAssigned;
    }
  }

  @Override
  public Map<String, Map<String, String>> reassignSegments(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, @Nullable InstancePartitionsType instancePartitionsType) {
    int numPartitions = instancePartitions.getNumPartitions();
    // TODO Preconditions to check if its COMPLETED
    Map<String, Map<String, String>> newAssignment;
    Map<Integer, List<String>> instancePartitionIdToSegmentsMap = new HashMap<>();
    for (String segmentName : currentAssignment.keySet()) {
      int partitionGroupId = getPartitionGroupId(segmentName);
      int instancePartitionId = partitionGroupId % numPartitions;
      instancePartitionIdToSegmentsMap.computeIfAbsent(instancePartitionId, k -> new ArrayList<>())
          .add(segmentName);
    }

    // NOTE: Shuffle the segments within the current assignment to avoid moving only new segments to the new added
    //       servers, which might cause hotspot servers because queries tend to hit the new segments. Use the table
    //       name hash as the random seed for the shuffle so that the result is deterministic.
    Random random = new Random(_tableName.hashCode());
    for (List<String> segments : instancePartitionIdToSegmentsMap.values()) {
      Collections.shuffle(segments, random);
    }

    newAssignment = SegmentAssignmentUtils
        .rebalanceReplicaGroupBasedTable(currentAssignment, instancePartitions, instancePartitionIdToSegmentsMap);
    return newAssignment;
  }

  private int getPartitionGroupId(String segmentName) {
    Integer segmentPartitionId =
        SegmentUtils.getRealtimeSegmentPartitionId(segmentName, _tableName, _helixManager, _partitionColumn);
    if (segmentPartitionId == null) {
      // This case is for the uploaded segments for which there's no partition information.
      // A random, but consistent, partition id is calculated based on the hash code of the segment name.
      // Note that '% 10K' is used to prevent having partition ids with large value which will be problematic later in
      // instance assignment formula.
      segmentPartitionId = Math.abs(segmentName.hashCode() % 10_000);
    }
    return segmentPartitionId;
  }
}
