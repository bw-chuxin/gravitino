package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.CatalogMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class CatalogMetaDamengProvider extends CatalogMetaBaseSQLProvider {
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
