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
package org.apache.pinot.controller.helix.core.assignment.segment.strategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class ReplicaGroupSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaGroupSegmentAssignmentStrategy.class);

  private static HelixManager _helixManager;
  private static String _tableName;
  private static String _partitionColumn;
  private int _replication;
  private TableConfig _tableConfig;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _tableConfig = tableConfig;
    _tableName = tableConfig.getTableName();
    _replication = tableConfig.getValidationConfig().getReplicationNumber();
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    _partitionColumn = replicaGroupStrategyConfig != null ? replicaGroupStrategyConfig.getPartitionColumn() : null;
    if (_partitionColumn == null) {
      LOGGER.info("Initialized ReplicaGroupSegmentAssignmentStrategy "
              + "with replication: {} without partition column for table: {} ", _replication, _tableName);
    } else {
      LOGGER.info("Initialized ReplicaGroupSegmentAssignmentStrategy "
              + "with replication: {} and partition column: {} for table: {}",
              _replication, _partitionColumn, _tableName);
    }
  }

  /**
   * Assigns the segment for the replica-group based segment assignment strategy and returns the assigned instances.
   */
  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, InstancePartitionsType instancePartitionsType) {
    int numPartitions = instancePartitions.getNumPartitions();
    SegmentAssignmentStrategyUtils.checkReplication(instancePartitions, _replication, _tableName);
    int partitionId;
    if (_partitionColumn == null || numPartitions == 1) {
      partitionId = 0;
    } else {
      // Uniformly spray the segment partitions over the instance partitions
      if (_tableConfig.getTableType() == TableType.OFFLINE) {
        partitionId =
            SegmentAssignmentUtils
                .getOfflineSegmentPartitionId(segmentName, _tableName, _helixManager, _partitionColumn) % numPartitions;
      } else {
        partitionId = SegmentAssignmentUtils
            .getRealtimeSegmentPartitionId(segmentName, _tableName, _helixManager, _partitionColumn) % numPartitions;
      }
    }
    return SegmentAssignmentUtils.assignSegmentWithReplicaGroup(currentAssignment, instancePartitions, partitionId);
  }

  @Override
  public Map<String, Map<String, String>> reassignSegments(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, InstancePartitionsType instancePartitionsType) {
    Map<String, Map<String, String>> newAssignment;
    int numPartitions = instancePartitions.getNumPartitions();

    SegmentAssignmentStrategyUtils.checkReplication(instancePartitions, _replication, _tableName);

    if (_partitionColumn == null || numPartitions == 1) {
      // NOTE: Shuffle the segments within the current assignment to avoid moving only new segments to the new added
      //       servers, which might cause hotspot servers because queries tend to hit the new segments. Use the
      //       table name hash as the random seed for the shuffle so that the result is deterministic.
      List<String> segments = new ArrayList<>(currentAssignment.keySet());
      Collections.shuffle(segments, new Random(_tableName.hashCode()));

      newAssignment = new TreeMap<>();
      SegmentAssignmentUtils.rebalanceReplicaGroupBasedPartition(currentAssignment, instancePartitions, 0, segments,
          newAssignment);
      return newAssignment;
    } else {
      Map<Integer, List<String>> instancePartitionIdToSegmentsMap;
      if (_tableConfig.getTableType() == TableType.OFFLINE) {
        instancePartitionIdToSegmentsMap = SegmentAssignmentUtils
            .getOfflineInstancePartitionIdToSegmentsMap(currentAssignment.keySet(),
                instancePartitions.getNumPartitions(), _tableName, _helixManager, _partitionColumn);
      } else {
        instancePartitionIdToSegmentsMap =
            SegmentAssignmentUtils
                .getRealtimeInstancePartitionIdToSegmentsMap(currentAssignment.keySet(),
                    instancePartitions.getNumPartitions(), _tableName, _helixManager, _partitionColumn);
      }

      // NOTE: Shuffle the segments within the current assignment to avoid moving only new segments to the new added
      //       servers, which might cause hotspot servers because queries tend to hit the new segments. Use the
      //       table name hash as the random seed for the shuffle so that the result is deterministic.
      Random random = new Random(_tableName.hashCode());
      for (List<String> segments : instancePartitionIdToSegmentsMap.values()) {
        Collections.shuffle(segments, random);
      }

      return SegmentAssignmentUtils
          .rebalanceReplicaGroupBasedTable(currentAssignment, instancePartitions, instancePartitionIdToSegmentsMap);
    }
  }
}
