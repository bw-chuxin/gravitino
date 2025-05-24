package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.SchemaMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class SchemaMetaDamengProvider extends SchemaMetaBaseSQLProvider {
  public String softDeleteSchemaMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String softDeleteSchemaMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteSchemaMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }
}
