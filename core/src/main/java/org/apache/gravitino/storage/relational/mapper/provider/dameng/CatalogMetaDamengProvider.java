package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.CatalogMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.ibatis.annotations.Param;

public class CatalogMetaDamengProvider extends CatalogMetaBaseSQLProvider {
  public String insertCatalogMetaOnDuplicateKeyUpdate(@Param("catalogMeta") CatalogPO catalogPO) {
    return "MERGE INTO "
        + TABLE_NAME
        + " AS target "
        + "USING (SELECT #{catalogMeta.catalogId} AS catalog_id) AS source "
        + "ON target.catalog_id = source.catalog_id "
        + "WHEN MATCHED THEN "
        + "UPDATE SET "
        + "target.catalog_name = #{catalogMeta.catalogName}, "
        + "target.metalake_id = #{catalogMeta.metalakeId}, "
        + "target.type = #{catalogMeta.type}, "
        + "target.provider = #{catalogMeta.provider}, "
        + "target.catalog_comment = #{catalogMeta.catalogComment}, "
        + "target.properties = #{catalogMeta.properties}, "
        + "target.audit_info = #{catalogMeta.auditInfo}, "
        + "target.current_version = #{catalogMeta.currentVersion}, "
        + "target.last_version = #{catalogMeta.lastVersion}, "
        + "target.deleted_at = #{catalogMeta.deletedAt} "
        + "WHEN NOT MATCHED THEN "
        + "INSERT ("
        + "catalog_id, catalog_name, metalake_id, type, provider, catalog_comment, properties, "
        + "audit_info, current_version, last_version, deleted_at"
        + ") VALUES ("
        + "#{catalogMeta.catalogId}, "
        + "#{catalogMeta.catalogName}, "
        + "#{catalogMeta.metalakeId}, "
        + "#{catalogMeta.type}, "
        + "#{catalogMeta.provider}, "
        + "#{catalogMeta.catalogComment}, "
        + "#{catalogMeta.properties}, "
        + "#{catalogMeta.auditInfo}, "
        + "#{catalogMeta.currentVersion}, "
        + "#{catalogMeta.lastVersion}, "
        + "#{catalogMeta.deletedAt}"
        + ")";
  }

  public String softDeleteCatalogMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteCatalogMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }
}
