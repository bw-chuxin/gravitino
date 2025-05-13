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

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;

/** Table operations for Dameng. */
public class DamengTableOperations extends JdbcTableOperations {
  private static final String BACK_QUOTE = "\"";
  private static final String ALTER_TABLE_PREFIX = "ALTER TABLE ";

  @Override
  public List<String> listTables(String databaseName) throws NoSuchSchemaException {
    try (Connection conn = getConnection(databaseName)) {
      DatabaseMetaData meta = conn.getMetaData();
      try (ResultSet rs =
          meta.getTables(null, databaseName, null, JdbcConnectorUtils.getTableTypes())) {
        List<String> tables = new ArrayList<>();
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
        }
        return tables;
      }
    } catch (SQLException e) {
      throw this.exceptionMapper.toGravitinoException(e);
    }
  }

  @Override
  public void create(
      String databaseName,
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes)
      throws TableAlreadyExistsException {
    LOG.info("Attempting to create table {} in database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      // create table
      JdbcConnectorUtils.executeUpdate(
          connection,
          cusGenerateCreateTableSql(
              databaseName,
              tableName,
              columns,
              comment,
              properties,
              partitioning,
              distribution,
              indexes));
      // add table comment
      JdbcConnectorUtils.executeUpdate(
          connection, generateTableCommentSql(databaseName, tableName, comment));
      // add column comments
      for (JdbcColumn column : columns) {
        if (StringUtils.isNotBlank(column.comment())) {
          JdbcConnectorUtils.executeUpdate(
              connection,
              generateColumnCommentSql(databaseName, tableName, column.name(), column.comment()));
        }
      }
      LOG.info("Created table {} in database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  protected String cusGenerateCreateTableSql(
      String databaseName,
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("CREATE TABLE ")
        .append(BACK_QUOTE)
        .append(databaseName)
        .append(BACK_QUOTE)
        .append(".")
        .append(BACK_QUOTE)
        .append(tableName)
        .append(BACK_QUOTE)
        .append(" (\n");
    // Add columns
    for (int i = 0; i < columns.length; i++) {
      JdbcColumn column = columns[i];
      sqlBuilder
          .append(SPACE)
          .append(SPACE)
          .append(BACK_QUOTE)
          .append(column.name())
          .append(BACK_QUOTE);

      appendColumnDefinition(column, sqlBuilder);
      // Add a comma for the next column, unless it's the last one
      if (i < columns.length - 1) {
        sqlBuilder.append(",\n");
      }
    }

    appendIndexesSql(indexes, sqlBuilder);

    sqlBuilder.append("\n)");
    return sqlBuilder.toString();
  }

  private void appendColumnDefinition(JdbcColumn column, StringBuilder sqlBuilder) {
    // Add data type
    sqlBuilder.append(SPACE).append(typeConverter.fromGravitino(column.dataType())).append(SPACE);

    // Add NOT NULL if the column is marked as such
    if (column.nullable()) {
      sqlBuilder.append("NULL ");
    } else {
      sqlBuilder.append("NOT NULL ");
    }

    // Add DEFAULT value if specified
    if (!DEFAULT_VALUE_NOT_SET.equals(column.defaultValue())) {
      sqlBuilder
          .append("DEFAULT ")
          .append(columnDefaultValueConverter.fromGravitino(column.defaultValue()))
          .append(SPACE);
    }

    // Add column auto_increment if specified
    if (column.autoIncrement()) {
      sqlBuilder.append("IDENTITY(1,1)").append(" ");
    }
  }

  public static void appendIndexesSql(Index[] indexes, StringBuilder sqlBuilder) {
    for (Index index : indexes) {
      String fieldStr = buildIndexFieldStr(index.fieldNames());
      sqlBuilder.append(",\n");
      switch (index.type()) {
        case PRIMARY_KEY:
          if (null != index.name()
              && !StringUtils.equalsIgnoreCase(
                  index.name(), Indexes.DEFAULT_MYSQL_PRIMARY_KEY_NAME)) {
            throw new IllegalArgumentException("Primary key name must be PRIMARY in Dameng");
          }
          sqlBuilder.append("NOT CLUSTER ").append("PRIMARY KEY (").append(fieldStr).append(")");
          break;
        case UNIQUE_KEY:
          sqlBuilder.append("CONSTRAINT ");
          if (null != index.name()) {
            sqlBuilder.append(BACK_QUOTE).append(index.name()).append(BACK_QUOTE);
          }
          sqlBuilder.append(" UNIQUE (").append(fieldStr).append(")");
          break;
        default:
          throw new IllegalArgumentException("Dameng doesn't support index : " + index.type());
      }
    }
  }

  private static String buildIndexFieldStr(String[][] strings) {
    return Stream.of(strings)
        .map(
            colNames -> {
              if (colNames.length > 1) {
                throw new IllegalArgumentException(
                    "Index does not support complex fields in this Catalog");
              }
              return wrapWithBackQuote(colNames[0]);
            })
        .collect(Collectors.joining(", "));
  }

  private String generateTableCommentSql(String databaseName, String tableName, String comment) {
    return String.format(
        "COMMENT ON TABLE %s.%s IS '%s'",
        BACK_QUOTE + databaseName + BACK_QUOTE, BACK_QUOTE + tableName + BACK_QUOTE, comment);
  }

  private String generateColumnCommentSql(
      String databaseName, String tableName, String name, String comment) {
    return String.format(
        "COMMENT ON COLUMN %s.%s.%s IS '%s'",
        BACK_QUOTE + databaseName + BACK_QUOTE,
        BACK_QUOTE + tableName + BACK_QUOTE,
        BACK_QUOTE + name + BACK_QUOTE,
        comment);
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException(
        "Dameng does not support purge table in Gravitino, please use drop table");
  }

  @Override
  protected void dropTable(String databaseName, String tableName) {
    LOG.info("Attempting to delete table {} from database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      JdbcConnectorUtils.executeUpdate(
          connection,
          String.format(
              "DROP TABLE %s.%s",
              BACK_QUOTE + databaseName + BACK_QUOTE, BACK_QUOTE + tableName + BACK_QUOTE));
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public void alterTable(String databaseName, String tableName, TableChange... changes)
      throws NoSuchTableException {
    LOG.info("Attempting to alter table {} from database {}", tableName, databaseName);
    try (Connection connection = getConnection(databaseName)) {
      for (TableChange change : changes) {
        String sql = generateAlterTableSql(databaseName, tableName, change);
        if (StringUtils.isEmpty(sql)) {
          LOG.info("No changes to alter table {} from database {}", tableName, databaseName);
          return;
        }
        JdbcConnectorUtils.executeUpdate(connection, sql);
      }
      LOG.info("Alter table {} from database {}", tableName, databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  protected String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes) {
    Preconditions.checkArgument(changes.length == 1, "Only one change is allowed at a time");
    TableChange change = changes[0];
    String tableId = generateTableId(databaseName, tableName);
    if (change instanceof TableChange.AddColumn) {
      TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
      return ALTER_TABLE_PREFIX + tableId + " ADD COLUMN " + addColumnFieldDefinition(addColumn);
    } else if (change instanceof TableChange.UpdateColumnComment) {
      TableChange.UpdateColumnComment updateColumnComment =
          (TableChange.UpdateColumnComment) change;
      Preconditions.checkArgument(updateColumnComment.getFieldName().length == 1);
      return "COMMENT ON COLUMN "
          + generateColumnId(databaseName, tableName, updateColumnComment.getFieldName()[0])
          + " IS '"
          + updateColumnComment.getNewComment()
          + "'";
    } else if (change instanceof TableChange.AddIndex) {
      TableChange.AddIndex addIndex = (TableChange.AddIndex) change;
      return generateAddIndexSql(databaseName, tableName, addIndex);
    } else if (change instanceof TableChange.DeleteColumn) {
      TableChange.DeleteColumn deleteColumn = (TableChange.DeleteColumn) change;
      return ALTER_TABLE_PREFIX
          + tableId
          + " DROP "
          + wrapWithBackQuote(deleteColumn.getFieldName()[0]);
    } else {
      throw new UnsupportedOperationException(
          "Dameng doesn't support alter table : " + change.getClass());
    }
  }

  private String addColumnFieldDefinition(TableChange.AddColumn addColumn) {
    StringBuilder sqlBuilder = new StringBuilder();
    Preconditions.checkArgument(addColumn.getFieldName().length == 1);

    String fieldName = addColumn.getFieldName()[0];

    sqlBuilder.append(BACK_QUOTE).append(fieldName).append(BACK_QUOTE);

    sqlBuilder
        .append(SPACE)
        .append(typeConverter.fromGravitino(addColumn.getDataType()))
        .append(SPACE);

    // Add NOT NULL if the column is marked as such
    if (addColumn.isNullable()) {
      sqlBuilder.append("NULL ");
    } else {
      sqlBuilder.append("NOT NULL ");
    }

    // Add DEFAULT value if specified
    if (!DEFAULT_VALUE_NOT_SET.equals(addColumn.getDefaultValue())) {
      sqlBuilder
          .append("DEFAULT ")
          .append(columnDefaultValueConverter.fromGravitino(addColumn.getDefaultValue()))
          .append(SPACE);
    }

    // Add column auto_increment if specified
    if (addColumn.isAutoIncrement()) {
      sqlBuilder.append("IDENTITY(1,1)").append(" ");
    }
    return sqlBuilder.toString();
  }

  private String generateAddIndexSql(
      String databaseName, String tableName, TableChange.AddIndex addIndex) {
    String tableId = generateTableId(databaseName, tableName);
    if (addIndex.getType() == Index.IndexType.PRIMARY_KEY) {
      return ALTER_TABLE_PREFIX
          + tableId
          + " ADD PRIMARY KEY ("
          + buildIndexFieldStr(addIndex.getFieldNames())
          + ")";
    } else if (addIndex.getType() == Index.IndexType.UNIQUE_KEY) {
      return ALTER_TABLE_PREFIX
          + tableId
          + " ADD CONSTRAINT "
          + wrapWithBackQuote(addIndex.getName())
          + " UNIQUE("
          + buildIndexFieldStr(addIndex.getFieldNames())
          + ")";
    } else {
      throw new IllegalArgumentException("Dameng doesn't support index : " + addIndex.getType());
    }
  }

  @Override
  protected ResultSet getTable(Connection connection, String databaseName, String tableName)
      throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getTables(null, databaseName, tableName, null);
  }

  @Override
  protected ResultSet getColumns(Connection connection, String databaseName, String tableName)
      throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getColumns(null, databaseName, tableName, null);
  }

  @Override
  protected JdbcColumn.Builder getBasicJdbcColumnInfo(ResultSet column) throws SQLException {
    JdbcTypeConverter.JdbcTypeBean typeBean =
        new JdbcTypeConverter.JdbcTypeBean(column.getString("TYPE_NAME"));
    typeBean.setColumnSize(column.getInt("COLUMN_SIZE"));
    typeBean.setScale(column.getInt("DECIMAL_DIGITS"));
    String comment = column.getString("REMARKS");
    boolean nullable = column.getBoolean("NULLABLE");

    String columnDef = column.getString("COLUMN_DEF");
    Expression defaultValue =
        columnDefaultValueConverter.toGravitino(typeBean, columnDef, false, nullable);

    return JdbcColumn.builder()
        .withName(column.getString("COLUMN_NAME"))
        .withType(typeConverter.toGravitino(typeBean))
        .withComment(StringUtils.isEmpty(comment) ? null : comment)
        .withNullable(nullable)
        .withDefaultValue(defaultValue);
  }

  @Override
  protected boolean getAutoIncrementInfo(ResultSet resultSet) throws SQLException {
    return false;
  }

  @Override
  protected ResultSet getPrimaryKeys(
      String databaseName, String tableName, DatabaseMetaData metaData) throws SQLException {
    return metaData.getPrimaryKeys(null, databaseName, tableName);
  }

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes) {
    throw new UnsupportedOperationException("Dameng does not support create table in Gravitino");
  }

  @Override
  protected ResultSet getIndexInfo(String databaseName, String tableName, DatabaseMetaData metaData)
      throws SQLException {
    return metaData.getIndexInfo(databaseName, databaseName, tableName, false, false);
  }

  private String generateTableId(String databaseName, String tableName) {
    return wrapWithBackQuote(databaseName) + "." + wrapWithBackQuote(tableName);
  }

  private String generateColumnId(String databaseName, String tableName, String columnName) {
    return generateTableId(databaseName, tableName) + "." + wrapWithBackQuote(columnName);
  }

  private static String wrapWithBackQuote(String identifier) {
    return BACK_QUOTE + identifier + BACK_QUOTE;
  }
}
