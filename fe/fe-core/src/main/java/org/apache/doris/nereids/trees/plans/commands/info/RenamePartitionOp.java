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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.analysis.PartitionRenameClause;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.Map;

/**
 * RenamePartitionOp
 */
public class RenamePartitionOp extends AlterTableOp {
    private String partitionName;
    private String newPartitionName;

    public RenamePartitionOp(String partitionName, String newPartitionName) {
        super(AlterOpType.RENAME);
        this.partitionName = partitionName;
        this.newPartitionName = newPartitionName;
        this.needTableStable = false;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getNewPartitionName() {
        return newPartitionName;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (Strings.isNullOrEmpty(partitionName)) {
            throw new AnalysisException("Partition name is not set");
        }

        if (Strings.isNullOrEmpty(newPartitionName)) {
            throw new AnalysisException("New partition name is not set");
        }

        FeNameFormat.checkPartitionName(newPartitionName);
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new PartitionRenameClause(partitionName, newPartitionName);
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        return "RENAME PARTITION " + partitionName + " " + newPartitionName;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
