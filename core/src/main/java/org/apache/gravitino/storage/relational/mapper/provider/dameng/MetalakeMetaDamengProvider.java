package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.MetalakeMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.ibatis.annotations.Param;

public class MetalakeMetaDamengProvider extends MetalakeMetaBaseSQLProvider {

  @Override
  public String insertMetalakeMetaOnDuplicateKeyUpdate(
      @Param("metalakeMeta") MetalakePO metalakePO) {
    return "MERGE INTO "
        + TABLE_NAME
        + " AS target "
        + "USING (SELECT #{metalakeMeta.metalakeId} AS metalake_id) AS source "
        + "ON target.metalake_id = source.metalake_id "
        + "WHEN MATCHED THEN "
        + "UPDATE SET "
        + "target.metalake_name = #{metalakeMeta.metalakeName}, "
        + "target.metalake_comment = #{metalakeMeta.metalakeComment}, "
        + "target.properties = #{metalakeMeta.properties}, "
        + "target.audit_info = #{metalakeMeta.auditInfo}, "
        + "target.schema_version = #{metalakeMeta.schemaVersion}, "
        + "target.current_version = #{metalakeMeta.currentVersion}, "
        + "target.last_version = #{metalakeMeta.lastVersion}, "
        + "target.deleted_at = #{metalakeMeta.deletedAt} "
        + "WHEN NOT MATCHED THEN "
        + "INSERT ("
        + "metalake_id, metalake_name, metalake_comment, properties, audit_info, "
        + "schema_version, current_version, last_version, deleted_at"
        + ") VALUES ("
        + "#{metalakeMeta.metalakeId}, "
        + "#{metalakeMeta.metalakeName}, "
        + "#{metalakeMeta.metalakeComment}, "
        + "#{metalakeMeta.properties}, "
        + "#{metalakeMeta.auditInfo}, "
        + "#{metalakeMeta.schemaVersion}, "
        + "#{metalakeMeta.currentVersion}, "
        + "#{metalakeMeta.lastVersion}, "
        + "#{metalakeMeta.deletedAt}"
        + ")";
  }

  public String softDeleteMetalakeMetaByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }
}
