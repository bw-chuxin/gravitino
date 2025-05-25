package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper.META_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.FilesetMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class FilesetMetaDamengProvider extends FilesetMetaBaseSQLProvider {
  public String softDeleteFilesetMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + META_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteFilesetMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + META_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteFilesetMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + META_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String softDeleteFilesetMetasByFilesetId(@Param("filesetId") Long filesetId) {
    return "UPDATE "
        + META_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE fileset_id = #{filesetId} AND deleted_at = 0";
  }
}
