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
package org.apache.gravitino.catalog.dameng.operation;

import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.MapUtils;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;

/** Database operations for Dameng. */
public class DamengDatabaseOperations extends JdbcDatabaseOperations {
  private static final String DATABASE_EXIST_SQL =
      "SELECT COUNT(*) FROM SYS.DBA_OBJECTS WHERE OBJECT_TYPE='SCH' AND OBJECT_NAME =?";

  @Override
  public void create(String databaseName, String comment, Map<String, String> properties)
      throws SchemaAlreadyExistsException {
    // create schema
    super.create(databaseName, comment, properties);
    // add comment to schema
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement(); ) {
      statement.execute(
          String.format("CALL SP_SET_SCHEMA_DESC('%s', '%s')", databaseName, comment));
    } catch (SQLException e) {
      throw this.exceptionMapper.toGravitinoException(e);
    }
  }

  @Override
  protected boolean supportSchemaComment() {
    return true;
  }

  @Override
  protected String generateCreateDatabaseSql(
      String databaseName, String comment, Map<String, String> properties) {
    String createDatabaseSql = String.format("CREATE SCHEMA \"%s\"", databaseName);
    if (MapUtils.isNotEmpty(properties)) {
      throw new UnsupportedOperationException("Properties are not supported yet.");
    }
    LOG.info("Generated create database:{} sql: {}", databaseName, createDatabaseSql);
    return createDatabaseSql;
  }

  @Override
  protected String generateDropDatabaseSql(String databaseName, boolean cascade) {
    String dropDatabaseSqlTemplate = "DROP SCHEMA \"%s\"";
    if (cascade) {
      return String.format(dropDatabaseSqlTemplate + " CASCADE", databaseName);
    } else {
      return String.format(dropDatabaseSqlTemplate, databaseName);
    }
  }

  @Override
  protected Set<String> createSysDatabaseNameSet() {
    return ImmutableSet.of("SYS");
  }

  @Override
  public List<String> listDatabases() {
    List<String> databaseNames = new ArrayList<>();
    try (Connection connection = getConnection(); ) {
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      ResultSet rs = databaseMetaData.getSchemas();
      while (rs.next()) {
        databaseNames.add(rs.getString(1));
      }
    } catch (SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
    return databaseNames;
  }

  @Override
  public boolean exist(String databaseName) {
    try (Connection connection = getConnection();
        PreparedStatement ps = connection.prepareStatement(DATABASE_EXIST_SQL); ) {
      ps.setString(1, databaseName);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getInt(1) > 0;
        }
      }
    } catch (SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
    return false;
  }
}
