package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.TableMetaMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.TableMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class TableMetaDamengProvider extends TableMetaBaseSQLProvider {
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
