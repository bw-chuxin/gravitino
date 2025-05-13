/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.dameng.converter;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/** Type converter for Dameng. */
public class DamengTypeConverter extends JdbcTypeConverter {
  @Override
  public String fromGravitino(Type type) {
    if (type instanceof Types.ByteType) {
      return "TINYINT";
    } else if (type instanceof Types.ShortType) {
      return "SMALLINT";
    } else if (type instanceof Types.IntegerType) {
      return "INT";
    } else if (type instanceof Types.LongType) {
      return "BIGINT";
    } else if (type instanceof Types.FloatType) {
      return type.simpleString();
    } else if (type instanceof Types.DoubleType) {
      return type.simpleString();
    } else if (type instanceof Types.StringType) {
      return "TEXT";
    } else if (type instanceof Types.DateType) {
      return type.simpleString();
    } else if (type instanceof Types.TimeType) {
      return type.simpleString();
    } else if (type instanceof Types.TimestampType) {
      return ((Types.TimestampType) type).hasTimeZone() ? "TIMESTAMP" : "DATETIME";
    } else if (type instanceof Types.DecimalType) {
      return type.simpleString();
    } else if (type instanceof Types.VarCharType) {
      return type.simpleString();
    } else if (type instanceof Types.FixedCharType) {
      return type.simpleString();
    } else if (type instanceof Types.BinaryType) {
      return type.simpleString();
    } else if (type instanceof Types.BooleanType) {
      return "BIT";
    } else if (type instanceof Types.ExternalType) {
      return ((Types.ExternalType) type).catalogString();
    }
    throw new IllegalArgumentException(
        String.format("Couldn't convert Gravitino type %s to MySQL type", type.simpleString()));
  }

  @Override
  public Type toGravitino(JdbcTypeBean typeBean) {
    switch (typeBean.getTypeName().toUpperCase()) {
      case "BIT":
        if (typeBean.getColumnSize() == null || typeBean.getColumnSize() == 1) {
          return Types.BooleanType.get();
        }
        return Types.BinaryType.get();
      case "TINYINT":
        return Types.ByteType.get();
      case "SMALLINT":
        return Types.ShortType.get();
      case "INT":
        return Types.IntegerType.get();
      case "BIGINT":
        return Types.LongType.get();
      case "FLOAT":
        return Types.FloatType.get();
      case "DOUBLE":
      case "DOUBLE PRECISION":
        return Types.DoubleType.get();
      case "DECIMAL":
      case "NUMERIC":
      case "NUMBER":
      case "DEC":
        return Types.DecimalType.of(typeBean.getColumnSize(), typeBean.getScale());
      case "VARCHAR":
        return Types.VarCharType.of(typeBean.getColumnSize());
      case "CHAR":
        return Types.FixedCharType.of(typeBean.getColumnSize());
      case "TEXT":
        return Types.StringType.get();
      case "DATE":
        return Types.DateType.get();
      case "TIME":
        return Types.TimeType.get();
      case "TIMESTAMP":
        return Types.TimestampType.withTimeZone();
      case "BINARY":
        return Types.BinaryType.get();
      default:
        return Types.ExternalType.of(typeBean.getTypeName());
    }
  }
}
