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
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OfflineAllServersSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(OfflineAllServersSegmentAssignmentStrategy.class);

  private static HelixManager _helixManager;
  private String _offlineTableName;
  private TenantConfig _tenantConfig;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _offlineTableName = tableConfig.getTableName();
    _tenantConfig = tableConfig.getTenantConfig();
    LOGGER.info("Initialized AllServersSegmentAssignmentStrategy for table: {}", _offlineTableName);
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, @Nullable InstancePartitionsType instancePartitionsType) {
    String serverTag = _tenantConfig.getServer();
    Set<String> instances = HelixHelper.getServerInstancesForTenant(_helixManager, serverTag);
    int numInstances = instances.size();
    Preconditions.checkState(numInstances > 0, "No instance found with tag: %s or %s",
        TagNameUtils.getOfflineTagForTenant(serverTag), TagNameUtils.getRealtimeTagForTenant(serverTag));
    return new ArrayList<>(instances);
  }

  @Override
  public Map<String, Map<String, String>> reassignSegments(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, @Nullable InstancePartitionsType instancePartitionsType) {
    String serverTag = _tenantConfig.getServer();
    Set<String> instances = HelixHelper.getServerInstancesForTenant(_helixManager, serverTag);
    Map<String, Map<String, String>> newAssignment = new TreeMap<>();
    for (String segment : currentAssignment.keySet()) {
      newAssignment.put(segment, SegmentAssignmentUtils
          .getInstanceStateMap(instances, CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE));
    }
    return newAssignment;
  }
}
