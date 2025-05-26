package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.TagMetaMapper.TAG_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.TagMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class TagMetaDamengProvider extends TagMetaBaseSQLProvider {
  public String softDeleteTagMetaByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return "UPDATE "
        + TAG_TABLE_NAME
        + " tm SET tm.deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE tm.metalake_id IN ("
        + " SELECT mm.metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND tm.tag_name = #{tagName} AND tm.deleted_at = 0";
  }

  public String softDeleteTagMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TAG_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }
}
