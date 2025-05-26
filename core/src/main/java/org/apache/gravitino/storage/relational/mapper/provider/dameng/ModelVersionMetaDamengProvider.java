package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionAliasRelMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.ModelVersionMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class ModelVersionMetaDamengProvider extends ModelVersionMetaBaseSQLProvider {
  public String softDeleteModelVersionsBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " mvi SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE mvi.schema_id = #{schemaId} AND mvi.model_id = ("
        + " SELECT mm.model_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mm WHERE mm.schema_id = #{schemaId} AND mm.model_name = #{modelName}"
        + " AND mm.deleted_at = 0) AND mvi.deleted_at = 0";
  }

  public String softDeleteModelVersionMetaByModelIdAndVersion(
      @Param("modelId") Long modelId, @Param("modelVersion") Integer modelVersion) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE model_id = #{modelId} AND version = #{modelVersion} AND deleted_at = 0";
  }

  public String softDeleteModelVersionMetaByModelIdAndAlias(
      @Param("modelId") Long modelId, @Param("alias") String alias) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE model_id = #{modelId} AND version = ("
        + " SELECT model_version FROM "
        + ModelVersionAliasRelMapper.TABLE_NAME
        + " WHERE model_id = #{modelId} AND model_version_alias = #{alias} AND deleted_at = 0)"
        + " AND deleted_at = 0";
  }

  public String softDeleteModelVersionMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String softDeleteModelVersionMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteModelVersionMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }
}
