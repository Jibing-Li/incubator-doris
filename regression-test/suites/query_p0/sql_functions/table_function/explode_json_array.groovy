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

// The cases is copied from 
// https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-lateral-view.html
// and modified by Doris.

suite("explode_json_array") {
    sql """ DROP TABLE IF EXISTS person """
    sql """
        CREATE TABLE IF NOT EXISTS person 
        (id INT, name STRING, age INT, class INT, address STRING) 
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8  
        PROPERTIES("replication_num" = "1")
    """

    sql """ INSERT INTO person VALUES
        (100, 'John', 30, 1, 'Street 1'),
        (200, 'Mary', NULL, 1, 'Street 2'),
        (300, 'Mike', 80, 3, 'Street 3'),
        (400, 'Dan', 50, 4, 'Street 4')  """
    qt_explode_json_array7 """ SELECT id, name, age, class, address, d_age, c_age FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[30, 60]') t1 as c_age 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[40, 80]') t2 as d_age 
                        ORDER BY id, c_age, d_age """

    qt_explode_json_array8 """ SELECT c_age, COUNT(1) FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[30, 60]') t1 as c_age 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[40, 80]') t2 as d_age 
                        GROUP BY c_age ORDER BY c_age """

    qt_explode_json_array_8_invalid """ SELECT c_age, COUNT(1) FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('["1", 60]') t1 as c_age 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('["b", "c"]') t2 as d_age 
                        GROUP BY c_age ORDER BY c_age """

    qt_explode_json_array9 """ SELECT * FROM person
                            LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[]') t1 AS c_age 
                            ORDER BY id, c_age """

    qt_explode_json_array10 """ SELECT id, name, age, class, address, d, c FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('[1, "b", -3]') t1 as c 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_DOUBLE('[1.23, 22.214, 214.1]') t2 as d 
                        ORDER BY id, c, d """

    qt_outer_join_explode_json_array11 """SELECT id, age, e1 FROM (SELECT id, age, e1 FROM (SELECT b.id, a.age FROM 
                                        person a LEFT JOIN person b ON a.id=b.age)T LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('[1, "b", 3]')
                                        TMP AS e1) AS T ORDER BY age, e1"""
    qt_outer_join_explode_json_array111 """SELECT id, age, e1 FROM (SELECT id, age, e1 FROM (SELECT b.id, a.age FROM
                                        person a LEFT JOIN person b ON a.id=b.age)T LATERAL VIEW EXPLODE_JSON_ARRAY_JSON('[{"id":1,"name":"John"},{"id":2,"name":"Mary"},{"id":3,"name":"Bob"}]')
                                        TMP AS e1) AS T ORDER BY age, e1"""

    qt_outer_join_explode_json_array112 """SELECT id, age, e1 FROM (SELECT id, age, e1 FROM (SELECT b.id, a.age FROM
                                        person a LEFT JOIN person b ON a.id=b.age)T LATERAL VIEW EXPLODE_JSON_ARRAY_JSON(cast('[{"id":1,"name":"John"},{"id":2,"name":"Mary"},{"id":3,"name":"Bob"}]' as Json))
                                        TMP AS e1) AS T ORDER BY age, cast(e1 as string)"""

    qt_explode_json_array12 """ SELECT c_age, COUNT(1) FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[9223372036854775807,9223372036854775808]') t1 as c_age 
                        GROUP BY c_age ORDER BY c_age """
    qt_explode_json_array122 """ SELECT c_age, COUNT(1) FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[9223372036854775808,9223372036854775809]') t1 as c_age 
                        GROUP BY c_age ORDER BY c_age """
    qt_explode_json_array13 """ SELECT c_age, COUNT(1) FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[-92233720368547758071,-92233720368547758081]') t1 as c_age 
                        GROUP BY c_age ORDER BY c_age """
    qt_explode_json_array132 """ SELECT c_age, COUNT(1) FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[-92233720368547758070,-92233720368547758082]') t1 as c_age 
                        GROUP BY c_age ORDER BY c_age """
    qt_explode_json_array133 """ SELECT c_age, COUNT(1) FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[123434243.7678,1232423437.876676274]') t1 as c_age 
                        GROUP BY c_age ORDER BY c_age """
    qt_explode_json_array134 """ SELECT c_age, COUNT(1) FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_INT('[9223372036854775808.7678,1232423437.876676274]') t1 as c_age 
                        GROUP BY c_age ORDER BY c_age """
    qt_explode_json_array14 """ SELECT id, name, age, class, address, d, c FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('[1182381637816312, "b", -1273982982312333]') t1 as c 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_DOUBLE('[1.23, 22.214, 214.1]') t2 as d 
                        ORDER BY id, c, d """

    qt_explode_json_array15 """ SELECT id, name, age, class, address, d, c FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('[true, "b", false]') t1 as c 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_DOUBLE('[1.23, 22.214, 214.1]') t2 as d 
                        ORDER BY id, c, d """    

    qt_explode_json_array16 """ SELECT id, name, age, class, address, d, c FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('[null, "b", null]') t1 as c 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_DOUBLE('[1.23, 22.214, 214.1]') t2 as d 
                        ORDER BY id, c, d """   
    qt_explode_json_array17 """ SELECT id, name, age, class, address, d, c FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_DOUBLE('[null, "b", null]') t1 as c 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_DOUBLE('[1123, 123, 432]') t2 as d 
                        ORDER BY id, c, d """  
    qt_explode_json_array18 """ SELECT id, name, age, class, address, d, c FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('[null, "b", null]') t1 as c 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('["1123", "123", "432"]') t2 as d 
                        ORDER BY id, c, d """ 
    qt_explode_json_array19 """ SELECT id, name, age, class, address, d, c FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('[null, "b", null]') t1 as c 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('["1123.123", "123.434", "432.756"]') t2 as d 
                        ORDER BY id, c, d """  
    qt_explode_json_array20 """ SELECT id, name, age, class, address, d, c FROM person
                        LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('[null, "b", null]') t1 as c 
                        LATERAL VIEW EXPLODE_JSON_ARRAY_STRING('["false", "true"]') t2 as d 
                        ORDER BY id, c, d """
    sql """ DROP TABLE IF EXISTS json_array_example """
    sql """
        CREATE TABLE json_array_example (
            id INT,
            json_array STRING
        )DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1");
    """   
    sql """ 
                INSERT INTO json_array_example (id, json_array) VALUES
            (1, '[1, 2, 3, 4, 5]'),
            (2, '[1.1, 2.2, 3.3, 4.4]'),
            (3, '["apple", "banana", "cherry"]'),
            (4, '[{"a": 1}, {"b": 2}, {"c": 3}]'),
            (5, '[]'),
            (6, 'NULL');
    """ 

    qt_explode_json_array17 """ 
        SELECT id, e1
        FROM json_array_example
        LATERAL VIEW EXPLODE_JSON_ARRAY_JSON_OUTER(json_array) tmp1 AS e1
        WHERE id = 4 order by id, e1;
    """ 

}
