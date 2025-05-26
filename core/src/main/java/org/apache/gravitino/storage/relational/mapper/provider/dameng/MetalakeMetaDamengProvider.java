package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.MetalakeMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class MetalakeMetaDamengProvider extends MetalakeMetaBaseSQLProvider {
  public String softDeleteMetalakeMetaByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }
}
