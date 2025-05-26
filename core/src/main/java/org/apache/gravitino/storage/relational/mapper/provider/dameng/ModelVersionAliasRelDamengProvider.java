package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionAliasRelMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.ModelVersionAliasRelBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class ModelVersionAliasRelDamengProvider extends ModelVersionAliasRelBaseSQLProvider {
  public String softDeleteModelVersionAliasRelsBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName) {
    return "UPDATE "
        + ModelVersionAliasRelMapper.TABLE_NAME
        + " mvar SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE mvar.model_id = ("
        + " SELECT mm.model_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mm WHERE mm.schema_id = #{schemaId} AND mm.model_name = #{modelName}"
        + " AND mm.deleted_at = 0) AND mvar.deleted_at = 0";
  }

  public String softDeleteModelVersionAliasRelsByModelIdAndVersion(
      @Param("modelId") Long modelId, @Param("modelVersion") Integer modelVersion) {
    return "UPDATE "
        + ModelVersionAliasRelMapper.TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE model_id = #{modelId} AND model_version = #{modelVersion} AND deleted_at = 0";
  }

  public String softDeleteModelVersionAliasRelsByModelIdAndAlias(
      @Param("modelId") Long modelId, @Param("alias") String alias) {
    return "UPDATE "
        + ModelVersionAliasRelMapper.TABLE_NAME
        + " mvar JOIN ("
        + " SELECT model_version FROM "
        + ModelVersionAliasRelMapper.TABLE_NAME
        + " WHERE model_id = #{modelId} AND model_version_alias = #{alias} AND deleted_at = 0)"
        + " subquery ON mvar.model_version = subquery.model_version"
        + " SET mvar.deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE mvar.model_id = #{modelId} AND mvar.deleted_at = 0";
  }

  public String softDeleteModelVersionAliasRelsBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + ModelVersionAliasRelMapper.TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE model_id IN ("
        + " SELECT model_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0) AND deleted_at = 0";
  }

  public String softDeleteModelVersionAliasRelsByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + ModelVersionAliasRelMapper.TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE model_id IN ("
        + " SELECT model_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0) AND deleted_at = 0";
  }

  public String softDeleteModelVersionAliasRelsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + ModelVersionAliasRelMapper.TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE model_id IN ("
        + " SELECT model_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0) AND deleted_at = 0";
  }
}
