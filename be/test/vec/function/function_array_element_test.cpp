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

#include <string>

#include "function_test_util.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

TEST(function_array_element_test, element_at) {
    std::string func_name = "element_at";
    TestArray empty_arr;

    // element_at(Array<Int32>, Int32)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_INT};

        TestArray vec = {Int32(1), Int32(2), Int32(3)};
        DataSet data_set = {
                {{vec, 0}, Null()},    {{vec, 1}, Int32(1)},     {{vec, 4}, Null()},
                {{vec, -1}, Int32(3)}, {{vec, -3}, Int32(1)},    {{vec, -4}, Null()},
                {{Null(), 1}, Null()}, {{empty_arr, 0}, Null()}, {{empty_arr, 1}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }

    // element_at(Array<Int8>, Int32)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_TINYINT,
                                    PrimitiveType::TYPE_INT};

        TestArray vec = {Int8(1), Int8(2), Int8(3)};
        DataSet data_set = {
                {{vec, 0}, Null()},    {{vec, 1}, Int8(1)},      {{vec, 4}, Null()},
                {{vec, -1}, Int8(3)},  {{vec, -3}, Int8(1)},     {{vec, -4}, Null()},
                {{Null(), 1}, Null()}, {{empty_arr, 0}, Null()}, {{empty_arr, 1}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }

    // element_at(Array<Int128>, Int64)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_LARGEINT,
                                    PrimitiveType::TYPE_BIGINT};

        TestArray vec = {Int128(1), Int128(2), Int128(3)};
        DataSet data_set = {{{vec, Int64(0)}, Null()},      {{vec, Int64(1)}, Int128(1)},
                            {{vec, Int64(4)}, Null()},      {{vec, Int64(-1)}, Int128(3)},
                            {{vec, Int64(-3)}, Int128(1)},  {{vec, Int64(-4)}, Null()},
                            {{Null(), Int64(1)}, Null()},   {{empty_arr, Int64(0)}, Null()},
                            {{empty_arr, Int64(1)}, Null()}};

        static_cast<void>(check_function<DataTypeInt128, true>(func_name, input_types, data_set));
    }

    // element_at(Array<Float64>, Int64)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DOUBLE,
                                    PrimitiveType::TYPE_BIGINT};

        TestArray vec = {double(1.11), double(2.22), double(3.33)};
        DataSet data_set = {{{vec, Int64(0)}, Null()},        {{vec, Int64(1)}, double(1.11)},
                            {{vec, Int64(4)}, Null()},        {{vec, Int64(-1)}, double(3.33)},
                            {{vec, Int64(-3)}, double(1.11)}, {{vec, Int64(-4)}, Null()},
                            {{Null(), Int64(1)}, Null()},     {{empty_arr, Int64(0)}, Null()},
                            {{empty_arr, Int64(1)}, Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }

    // element_at(Array<DateTime>, Int64)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DATETIME,
                                    PrimitiveType::TYPE_BIGINT};

        TestArray vec = {std::string("2022-01-02 01:00:00"), std::string(""),
                         std::string("2022-07-08 03:00:00")};
        DataSet data_set = {{{vec, Int64(0)}, Null()},
                            {{vec, Int64(1)}, std::string("2022-01-02 01:00:00")},
                            {{vec, Int64(4)}, Null()},
                            {{vec, Int64(-1)}, std::string("2022-07-08 03:00:00")},
                            {{vec, Int64(-2)}, std::string("")},
                            {{vec, Int64(-4)}, Null()},
                            {{Null(), Int64(1)}, Null()},
                            {{empty_arr, Int64(0)}, Null()},
                            {{empty_arr, Int64(1)}, Null()}};

        static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
    }

    // element_at(Array<Date>, Int64)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DATE,
                                    PrimitiveType::TYPE_BIGINT};

        TestArray vec = {std::string("2022-01-02"), std::string(""), std::string("2022-07-08")};
        DataSet data_set = {
                {{vec, Int64(0)}, Null()},           {{vec, Int64(1)}, std::string("2022-01-02")},
                {{vec, Int64(4)}, Null()},           {{vec, Int64(-1)}, std::string("2022-07-08")},
                {{vec, Int64(-2)}, std::string("")}, {{vec, Int64(-4)}, Null()},
                {{Null(), Int64(1)}, Null()},        {{empty_arr, Int64(0)}, Null()},
                {{empty_arr, Int64(1)}, Null()}};

        static_cast<void>(check_function<DataTypeDate, true>(func_name, input_types, data_set));
    }

    // element_at(Array<Decimal128V2>, Int64)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_DECIMALV2,
                                    PrimitiveType::TYPE_BIGINT};

        TestArray vec = {ut_type::DECIMALV2(17014116.67), ut_type::DECIMALV2(-17014116.67),
                         ut_type::DECIMALV2(0.0)};
        DataSet data_set = {{{vec, Int64(0)}, Null()},
                            {{vec, Int64(1)}, ut_type::DECIMALV2(17014116.67)},
                            {{vec, Int64(4)}, Null()},
                            {{vec, Int64(-1)}, ut_type::DECIMALV2(0.0)},
                            {{vec, Int64(-2)}, ut_type::DECIMALV2(-17014116.67)},
                            {{vec, Int64(-4)}, Null()},
                            {{Null(), Int64(1)}, Null()},
                            {{empty_arr, Int64(0)}, Null()},
                            {{empty_arr, Int64(1)}, Null()}};

        static_cast<void>(
                check_function<DataTypeDecimalV2, true>(func_name, input_types, data_set, 9, 27));
    }

    // element_at(Array<String>, Int32)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_VARCHAR,
                                    PrimitiveType::TYPE_INT};

        TestArray vec = {std::string("abc"), std::string(""), std::string("def")};
        DataSet data_set = {{{vec, 1}, std::string("abc")},
                            {{vec, 2}, std::string("")},
                            {{vec, 10}, Null()},
                            {{vec, -2}, std::string("")},
                            {{vec, 0}, Null()},
                            {{vec, -10}, Null()},
                            {{Null(), 1}, Null()},
                            {{empty_arr, 0}, Null()},
                            {{empty_arr, 1}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

} // namespace doris::vectorized
