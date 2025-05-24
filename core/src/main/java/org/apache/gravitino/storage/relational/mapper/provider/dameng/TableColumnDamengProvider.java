package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.TableColumnBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class TableColumnDamengProvider extends TableColumnBaseSQLProvider {
  public String softDeleteColumnsByTableId(@Param("tableId") Long tableId) {
    return "UPDATE "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE table_id = #{tableId} AND deleted_at = 0";
  }

  public String softDeleteColumnsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteColumnsByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteColumnsBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }
}
