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

suite("test_map_select_with_limit", "query") {
    sql """
            CREATE TABLE IF NOT EXISTS test_map_select_with_limit (
              `k1` INT(11) NULL,
              `k2` MAP<SMALLINT(6), STRING> NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2",
            "disable_auto_compaction" = "false"
            )
            """
    // prepare data
    sql """ 
        INSERT INTO test_map_select_with_limit VALUES (100, {1: "amory", 2: "is", 3: "better"}), (101, {1: "amory", 2: "is", 3: "better"});
        alter table test_map_select_with_limit modify column k1 set stats ('ndv'='41700404', 'num_nulls'='0', 'min_value'='810', 'max_value'='602901', 'row_count'='1500000000');
        """
    // set topn_opt_limit_threshold = 1024 to make sure _internal_service to be request with proto request
    sql """ set topn_opt_limit_threshold = 1024 """
    explain{
        sql("select * from test_map_select_with_limit order by k1 limit 1")
        contains "TOPN"
    }


    qt_select """ select * from test_map_select_with_limit order by k1 limit 1 """
}