package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.TableMetaMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.TableMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.ibatis.annotations.Param;

public class TableMetaDamengProvider extends TableMetaBaseSQLProvider {
  public String insertTableMetaOnDuplicateKeyUpdate(@Param("tableMeta") TablePO tablePO) {
    return "MERGE INTO "
        + TABLE_NAME
        + " AS target "
        + "USING (SELECT #{tableMeta.tableId} AS table_id) AS source "
        + "ON target.table_id = source.table_id "
        + "WHEN MATCHED THEN "
        + "UPDATE SET "
        + "target.table_name = #{tableMeta.tableName}, "
        + "target.metalake_id = #{tableMeta.metalakeId}, "
        + "target.catalog_id = #{tableMeta.catalogId}, "
        + "target.schema_id = #{tableMeta.schemaId}, "
        + "target.audit_info = #{tableMeta.auditInfo}, "
        + "target.current_version = #{tableMeta.currentVersion}, "
        + "target.last_version = #{tableMeta.lastVersion}, "
        + "target.deleted_at = #{tableMeta.deletedAt} "
        + "WHEN NOT MATCHED THEN "
        + "INSERT ("
        + "table_id, table_name, metalake_id, catalog_id, schema_id, audit_info, "
        + "current_version, last_version, deleted_at"
        + ") VALUES ("
        + "#{tableMeta.tableId}, "
        + "#{tableMeta.tableName}, "
        + "#{tableMeta.metalakeId}, "
        + "#{tableMeta.catalogId}, "
        + "#{tableMeta.schemaId}, "
        + "#{tableMeta.auditInfo}, "
        + "#{tableMeta.currentVersion}, "
        + "#{tableMeta.lastVersion}, "
        + "#{tableMeta.deletedAt}"
        + ")";
  }

  public String softDeleteTableMetasByTableId(@Param("tableId") Long tableId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE table_id = #{tableId} AND deleted_at = 0";
  }

  public String softDeleteTableMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteTableMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteTableMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }
}
