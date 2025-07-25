// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Physical file scan for external catalog.
 */
public class PhysicalFileScan extends PhysicalCatalogRelation {

    protected final DistributionSpec distributionSpec;
    protected final SelectedPartitions selectedPartitions;
    protected final Optional<TableSample> tableSample;
    protected final Optional<TableSnapshot> tableSnapshot;
    protected final Optional<TableScanParams> scanParams;

    /**
     * Constructor for PhysicalFileScan.
     */
    public PhysicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            SelectedPartitions selectedPartitions, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot,
            Collection<Slot> operativeSlots,
            Optional<TableScanParams> scanParams) {
        this(id, PlanType.PHYSICAL_FILE_SCAN, table, qualifier, distributionSpec, groupExpression,
                logicalProperties, selectedPartitions, tableSample, tableSnapshot, operativeSlots, scanParams);
    }

    /**
     * Constructor for PhysicalFileScan.
     */
    public PhysicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, SelectedPartitions selectedPartitions,
            Optional<TableSample> tableSample, Optional<TableSnapshot> tableSnapshot,
            Collection<Slot> operativeSlots, Optional<TableScanParams> scanParams) {
        this(id, PlanType.PHYSICAL_FILE_SCAN, table, qualifier, distributionSpec, groupExpression,
                logicalProperties, physicalProperties, statistics, selectedPartitions, tableSample, tableSnapshot,
                operativeSlots, scanParams);
    }

    /**
     * For hudi file scan to specified PlanTye
     */
    protected PhysicalFileScan(RelationId id, PlanType type, ExternalTable table, List<String> qualifier,
            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            SelectedPartitions selectedPartitions, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot,
            Collection<Slot> operativeSlots,
            Optional<TableScanParams> scanParams) {
        super(id, type, table, qualifier, groupExpression, logicalProperties, operativeSlots);
        this.distributionSpec = distributionSpec;
        this.selectedPartitions = selectedPartitions;
        this.tableSample = tableSample;
        this.tableSnapshot = tableSnapshot;
        this.scanParams = scanParams;
    }

    protected PhysicalFileScan(RelationId id, PlanType type, ExternalTable table, List<String> qualifier,
            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, SelectedPartitions selectedPartitions,
            Optional<TableSample> tableSample, Optional<TableSnapshot> tableSnapshot,
            Collection<Slot> operativeSlots, Optional<TableScanParams> scanParams) {
        super(id, type, table, qualifier, groupExpression, logicalProperties,
                physicalProperties, statistics, operativeSlots);
        this.distributionSpec = distributionSpec;
        this.selectedPartitions = selectedPartitions;
        this.tableSample = tableSample;
        this.tableSnapshot = tableSnapshot;
        this.scanParams = scanParams;
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
    }

    public SelectedPartitions getSelectedPartitions() {
        return selectedPartitions;
    }

    public Optional<TableSample> getTableSample() {
        return tableSample;
    }

    public Optional<TableSnapshot> getTableSnapshot() {
        return tableSnapshot;
    }

    public Optional<TableScanParams> getScanParams() {
        return scanParams;
    }

    @Override
    public String toString() {
        String rfV2 = "";
        if (!runtimeFiltersV2.isEmpty()) {
            rfV2 = runtimeFiltersV2.toString();
        }
        return Utils.toSqlString("PhysicalFileScan[" + table.getName() + "]" + getGroupIdWithPrefix(),
            "stats", statistics,
                "qualified", Utils.qualifiedName(qualifier, table.getName()),
                "selected partitions num",
                selectedPartitions.isPruned ? selectedPartitions.selectedPartitions.size() : "unknown",
                "operativeCols", getOperativeSlots(), "RFV2", rfV2
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalFileScan(this, context);
    }

    @Override
    public PhysicalFileScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalFileScan(relationId, getTable(), qualifier, distributionSpec,
                groupExpression, getLogicalProperties(), selectedPartitions, tableSample, tableSnapshot,
                operativeSlots, scanParams);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalFileScan(relationId, getTable(), qualifier, distributionSpec,
                groupExpression, logicalProperties.get(), selectedPartitions, tableSample, tableSnapshot,
                operativeSlots, scanParams);
    }

    @Override
    public ExternalTable getTable() {
        return (ExternalTable) table;
    }

    @Override
    public PhysicalFileScan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                       Statistics statistics) {
        return new PhysicalFileScan(relationId, getTable(), qualifier, distributionSpec,
                groupExpression, getLogicalProperties(), physicalProperties, statistics,
                selectedPartitions, tableSample, tableSnapshot,
                operativeSlots, scanParams);
    }

    @Override
    public List<Slot> getOperativeSlots() {
        return operativeSlots;
    }
}
