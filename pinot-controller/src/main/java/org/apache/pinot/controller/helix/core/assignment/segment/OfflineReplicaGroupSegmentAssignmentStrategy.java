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
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class OfflineReplicaGroupSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(OfflineReplicaGroupSegmentAssignmentStrategy.class);

  private static HelixManager _helixManager;
  private static String _offlineTableName;
  private static String _partitionColumn;
  private int _replication;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _offlineTableName = tableConfig.getTableName();
    _replication = tableConfig.getValidationConfig().getReplicationNumber();
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    _partitionColumn = replicaGroupStrategyConfig != null ? replicaGroupStrategyConfig.getPartitionColumn() : null;
    if (_partitionColumn == null) {
      LOGGER.info("Initialized OfflineReplicaGroupSegmentAssignmentStrategy with replication: {} without partition column for table: {} ",
          _replication, _offlineTableName);
    } else {
      LOGGER.info("Initialized OfflineReplicaGroupSegmentAssignmentStrategy with replication: {} and partition column: {} for table: {}",
          _replication, _partitionColumn, _offlineTableName);
    }
  }

  /**
   * Assigns the segment for the replica-group based segment assignment strategy and returns the assigned instances.
   */
  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, @Nullable InstancePartitionsType instancePartitionsType) {
    // Fetch partition id from segment ZK metadata if partition column is configured
    int partitionId;
    if (_partitionColumn == null) {
      partitionId = 0;
    } else {
      SegmentZKMetadata segmentZKMetadata = ZKMetadataProvider
          .getSegmentZKMetadata(_helixManager.getHelixPropertyStore(), _offlineTableName, segmentName);
      Preconditions
          .checkState(segmentZKMetadata != null, "Failed to find segment ZK metadata for segment: %s of table: %s",
              segmentName, _offlineTableName);
      int segmentPartitionId =
          SegmentAssignmentStrategyUtils.getPartitionId(segmentZKMetadata, _partitionColumn, _offlineTableName);

      // Uniformly spray the segment partitions over the instance partitions
      int numPartitions = instancePartitions.getNumPartitions();
      partitionId = segmentPartitionId % numPartitions;
    }

    return SegmentAssignmentUtils.assignSegmentWithReplicaGroup(currentAssignment, instancePartitions, partitionId);
  }

  @Override
  public Map<String, Map<String, String>> reassignSegments(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, @Nullable InstancePartitionsType instancePartitionsType) {
    Map<String, Map<String, String>> newAssignment;
    if (_partitionColumn == null) {
      // NOTE: Shuffle the segments within the current assignment to avoid moving only new segments to the new added
      //       servers, which might cause hotspot servers because queries tend to hit the new segments. Use the
      //       table name hash as the random seed for the shuffle so that the result is deterministic.
      List<String> segments = new ArrayList<>(currentAssignment.keySet());
      Collections.shuffle(segments, new Random(_offlineTableName.hashCode()));

      newAssignment = new TreeMap<>();
      SegmentAssignmentUtils
          .rebalanceReplicaGroupBasedPartition(currentAssignment, instancePartitions, 0, segments, newAssignment);
    } else {
      newAssignment = rebalanceTableWithPartition(currentAssignment, instancePartitions);
    }
    return newAssignment;
  }

  private Map<String, Map<String, String>> rebalanceTableWithPartition(
      Map<String, Map<String, String>> currentAssignment, InstancePartitions instancePartitions) {
    // Fetch partition id from segment ZK metadata
    List<SegmentZKMetadata> segmentsZKMetadata =
        ZKMetadataProvider.getSegmentsZKMetadata(_helixManager.getHelixPropertyStore(), _offlineTableName);
    Map<String, SegmentZKMetadata> segmentZKMetadataMap = new HashMap<>();
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      segmentZKMetadataMap.put(segmentZKMetadata.getSegmentName(), segmentZKMetadata);
    }
    int numPartitions = instancePartitions.getNumPartitions();
    Map<Integer, List<String>> instancePartitionIdToSegmentsMap = new HashMap<>();
    for (String segmentName : currentAssignment.keySet()) {
      int partitionId = getPartitionId(segmentZKMetadataMap.get(segmentName));
      int instancePartitionId = partitionId % numPartitions;
      instancePartitionIdToSegmentsMap.computeIfAbsent(instancePartitionId, k -> new ArrayList<>()).add(segmentName);
    }

    // NOTE: Shuffle the segments within the current assignment to avoid moving only new segments to the new added
    //       servers, which might cause hotspot servers because queries tend to hit the new segments. Use the table
    //       name hash as the random seed for the shuffle so that the result is deterministic.
    Random random = new Random(_offlineTableName.hashCode());
    for (List<String> segments : instancePartitionIdToSegmentsMap.values()) {
      Collections.shuffle(segments, random);
    }

    return SegmentAssignmentUtils
        .rebalanceReplicaGroupBasedTable(currentAssignment, instancePartitions, instancePartitionIdToSegmentsMap);
  }

  private int getPartitionId(SegmentZKMetadata segmentZKMetadata) {
    String segmentName = segmentZKMetadata.getSegmentName();
    ColumnPartitionMetadata partitionMetadata =
        segmentZKMetadata.getPartitionMetadata().getColumnPartitionMap().get(_partitionColumn);
    Preconditions.checkState(partitionMetadata != null,
        "Segment ZK metadata for segment: %s of table: %s does not contain partition metadata for column: %s",
        segmentName, _offlineTableName, _partitionColumn);
    Set<Integer> partitions = partitionMetadata.getPartitions();
    Preconditions.checkState(partitions.size() == 1,
        "Segment ZK metadata for segment: %s of table: %s contains multiple partitions for column: %s", segmentName,
        _offlineTableName, _partitionColumn);
    return partitions.iterator().next();
  }
}
