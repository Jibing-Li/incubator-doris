package mv.external_table
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

suite("single_external_table", "p0,external,hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test. then doesn't test mv rewrite")
        return;
    }
    // prepare catalog
    def suite_name = "single_external_table";
    def externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def hms_port = context.config.otherConfigs.get("hive2HmsPort")
    def hive_catalog_name = "${suite_name}_catalog"
    def hive_database = "${suite_name}_db"
    def hive_table = "${suite_name}_orders"

    sql """drop catalog if exists ${hive_catalog_name}"""
    sql """
    create catalog if not exists ${hive_catalog_name} properties (
        "type"="hms",
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
    );"""

    sql """switch ${hive_catalog_name};"""
    sql """ drop database if exists ${hive_database} force"""
    sql """ create database ${hive_database}"""
    sql """use ${hive_database}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${hive_table}  (
              o_orderkey       integer,
              o_custkey        integer,
              o_orderstatus    char(1),
              o_totalprice     decimalv3(15,2),
              o_orderpriority  char(15),  
              o_clerk          char(15), 
              o_shippriority   integer,
              o_comment        varchar(79),
              o_orderdate      date
            ) ENGINE=hive
            PARTITION BY list(o_orderdate)()
            PROPERTIES (
              "replication_num" = "1",
              "file_format"="orc",
              "compression"="zlib"
            );
            """

    sql """insert into ${hive_catalog_name}.${hive_database}.${hive_table} values(1, 1, 'ok', 99.5, 'a', 'b', 1, 'yy', '2023-10-17');"""
    sql """insert into ${hive_catalog_name}.${hive_database}.${hive_table} values(2, 2, 'ok', 109.2, 'c','d',2, 'mm', '2023-10-18');"""
    sql """insert into ${hive_catalog_name}.${hive_database}.${hive_table} values(3, 3, 'ok', 99.5, 'a', 'b', 1, 'yy', '2023-10-19');"""

    // prepare table and data in olap
    def internal_catalog = "internal"
    def olap_db = context.config.getDbNameByFile(context.file)

    sql """switch ${internal_catalog};"""
    sql "use ${olap_db};"
    sql "SET enable_nereids_planner=true;"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject';"
    sql "SET materialized_view_rewrite_enable_contain_external_table=true"


    // single table without aggregate
    def mv1_0 = """
            select  o_custkey, o_orderdate 
            from ${hive_catalog_name}.${hive_database}.${hive_table};
    """
    def query1_0 = """
            select o_custkey 
            from ${hive_catalog_name}.${hive_database}.${hive_table};
            """
    order_qt_query1_0_before "${query1_0}"
    // todo after add cost on external table, change to check
    async_mv_rewrite_success_without_check_chosen(olap_db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""


    // single table filter without aggregate
    def mv1_1 = """
            select o_custkey, o_orderdate 
            from ${hive_catalog_name}.${hive_database}.${hive_table} 
            where o_custkey > 1;
    """
    def query1_1 = """
            select o_custkey 
            from ${hive_catalog_name}.${hive_database}.${hive_table} 
            where o_custkey > 2;
            """
    order_qt_query1_1_before "${query1_1}"
    // todo after add cost on external table, change to check
    async_mv_rewrite_success_without_check_chosen(olap_db, mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""

    // single table and only query with filter
    def mv1_2 = """
            select o_custkey, o_orderdate 
            from ${hive_catalog_name}.${hive_database}.${hive_table} 
    """
    def query1_2 = """
            select o_custkey 
            from ${hive_catalog_name}.${hive_database}.${hive_table} 
            where o_custkey > 2;
            """
    order_qt_query1_2_before "${query1_2}"
    async_mv_rewrite_success(olap_db, mv1_2, query1_2, "mv1_2")
    order_qt_query1_2_after "${query1_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_2"""



    // single table with aggregate and filter
    def mv1_3 = """
            select o_custkey, o_orderdate,
            count(*)
            from ${hive_catalog_name}.${hive_database}.${hive_table} 
            group by
            o_custkey, o_orderdate;
    """
    def query1_3 = """
            select o_custkey, o_orderdate,
            count(*)
            from ${hive_catalog_name}.${hive_database}.${hive_table} 
            where o_custkey > 2
            group by
            o_custkey, o_orderdate;
            """
    order_qt_query1_3_before "${query1_3}"
    async_mv_rewrite_success(olap_db, mv1_3, query1_3, "mv1_3")
    order_qt_query1_3_after "${query1_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3"""


    // single table with aggregate roll up and filter
    def mv1_4 = """
            select o_custkey, o_orderdate,
            count(*)
            from ${hive_catalog_name}.${hive_database}.${hive_table} 
            group by
            o_custkey, o_orderdate;
    """
    def query1_4 = """
            select o_custkey, o_orderdate,
            count(*)
            from ${hive_catalog_name}.${hive_database}.${hive_table} 
            where o_custkey > 2
            group by
            o_custkey, o_orderdate;
            """
    order_qt_query1_4_before "${query1_4}"
    async_mv_rewrite_success(olap_db, mv1_4, query1_4, "mv1_4")
    order_qt_query1_4_after "${query1_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_4"""



    // single table with aggregate roll up and filter
    def mv1_5 = """
            select o_custkey, o_orderdate, o_totalprice 
            from ${hive_catalog_name}.${hive_database}.${hive_table} 
    """
    def query1_5 = """
            select o_custkey, o_orderdate,
            count(*)
            from ${hive_catalog_name}.${hive_database}.${hive_table} 
            where o_custkey > 2
            group by
            o_custkey, o_orderdate;
            """
    order_qt_query1_5_before "${query1_5}"
    async_mv_rewrite_success(olap_db, mv1_5, query1_5, "mv1_5")
    order_qt_query1_5_after "${query1_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_5"""


    sql """drop database if exists ${hive_catalog_name}.${hive_database} force"""
    sql """drop catalog if exists ${hive_catalog_name}"""
}
