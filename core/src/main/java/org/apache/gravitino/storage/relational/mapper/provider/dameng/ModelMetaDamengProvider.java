package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.ModelMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class ModelMetaDamengProvider extends ModelMetaBaseSQLProvider {
  public String softDeleteModelMetaBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName) {
    return "UPDATE "
        + ModelMetaMapper.TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE schema_id = #{schemaId} AND model_name = #{modelName} AND deleted_at = 0";
  }

  public String softDeleteModelMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + ModelMetaMapper.TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteModelMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + ModelMetaMapper.TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteModelMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + ModelMetaMapper.TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }
}
