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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StatisticsJobAppender extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(StatisticsJobAppender.class);

    public static final long INTERVAL = 1000;

    public enum Priority {
        HIGH,
        LOW
    }

    private final Priority priority;

    public StatisticsJobAppender(String name, Priority priority) {
        super(name, INTERVAL);
        this.priority = priority;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Env.getCurrentEnv().isMaster()) {
            return;
        }
        if (!StatisticsUtil.statsTblAvailable()) {
            LOG.info("Stats table not available, skip");
            return;
        }
        if (Env.getCurrentEnv().getStatisticsAutoCollector() == null) {
            LOG.info("Statistics auto collector not ready, skip");
            return;
        }
        if (Env.isCheckpointThread()) {
            return;
        }
        if (Priority.HIGH.equals(priority)) {
            appendHighPriorityJob();
        } else if (Priority.LOW.equals(priority)) {
            appendLowPriorityJob();
        } else {
            LOG.warn("Appender priority is invalid. {}", priority);
        }
    }

    private void appendHighPriorityJob() {

    }

    private void appendLowPriorityJob() {

    }

}
