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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.qe.ConnectContext;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * StringLikeLiteral.
 */
public abstract class StringLikeLiteral extends Literal implements ComparableLiteral {
    public static final int CHINESE_CHAR_BYTE_LENGTH = 4;
    public final String value;

    public StringLikeLiteral(String value, DataType dataType) {
        super(dataType);
        this.value = value;
    }

    public String getStringValue() {
        return value;
    }

    @Override
    public double getDouble() {
        return getDouble(value);
    }

    /**
     * get double value
     */
    public static double getDouble(String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        long v = 0;
        int pos = 0;
        int len = Math.min(bytes.length, 7);
        while (pos < len) {
            v += Byte.toUnsignedLong(bytes[pos]) << ((6 - pos) * 8);
            pos++;
        }
        return (double) v;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.StringLiteral(value);
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        boolean strictCast = ConnectContext.get().getSessionVariable().enableStrictCast();
        if (targetType instanceof IntegralType) {
            String regex = strictCast ? "\\s*[+-]?\\d+\\s*" : "\\s*[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)\\s*";
            Pattern pattern = Pattern.compile(regex);
            if (pattern.matcher(value).matches() && !numericOverflow(value.trim(), targetType)) {
                String trimmedValue = value.trim();
                trimmedValue = trimmedValue.startsWith(".") || trimmedValue.startsWith("+.") || trimmedValue.startsWith("-.")
                        ? "0" : trimmedValue.split("\\.")[0];
                if (targetType.isTinyIntType()) {
                    return Literal.of(Byte.valueOf(trimmedValue));
                } else if (targetType.isSmallIntType()) {
                    return Literal.of(Short.valueOf(trimmedValue));
                } else if (targetType.isIntegerType()) {
                    return Literal.of(Integer.valueOf(trimmedValue));
                } else if (targetType.isBigIntType()) {
                    return Literal.of(Long.valueOf(trimmedValue));
                } else if (targetType.isLargeIntType()) {
                    return Literal.of(new BigDecimal(trimmedValue).toBigInteger());
                }
            } else if (strictCast) {
                throw new AnalysisException(String.format("%s can't cast to %s in strict mode.", value, targetType));
            } else {
                return new NullLiteral(targetType);
            }
        }
        return super.uncheckedCastTo(targetType);
    }

    @Override
    public int compareTo(ComparableLiteral other) {
        if (other instanceof StringLikeLiteral) {
            // compare string with utf-8 byte array, same with DM,BE,StorageEngine
            byte[] thisBytes = null;
            byte[] otherBytes = null;
            try {
                thisBytes = getStringValue().getBytes("UTF-8");
                otherBytes = ((Literal) other).getStringValue().getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new AnalysisException(e.getMessage(), e);
            }

            int minLength = Math.min(thisBytes.length, otherBytes.length);
            int i = 0;
            for (i = 0; i < minLength; i++) {
                if (Byte.toUnsignedInt(thisBytes[i]) < Byte.toUnsignedInt(otherBytes[i])) {
                    return -1;
                } else if (Byte.toUnsignedInt(thisBytes[i]) > Byte.toUnsignedInt(otherBytes[i])) {
                    return 1;
                }
            }
            if (thisBytes.length > otherBytes.length) {
                if (thisBytes[i] == 0x00) {
                    return 0;
                } else {
                    return 1;
                }
            } else if (thisBytes.length < otherBytes.length) {
                if (otherBytes[i] == 0x00) {
                    return 0;
                } else {
                    return -1;
                }
            } else {
                return 0;
            }
        }
        if (other instanceof NullLiteral) {
            return 1;
        }
        if (other instanceof MaxLiteral) {
            return -1;
        }
        throw new RuntimeException("Cannot compare two values with different data types: "
                + this + " (" + dataType + ") vs " + other + " (" + ((Literal) other).dataType + ")");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StringLikeLiteral)) {
            return false;
        }
        StringLikeLiteral that = (StringLikeLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public String toString() {
        return "'" + value + "'";
    }
}
