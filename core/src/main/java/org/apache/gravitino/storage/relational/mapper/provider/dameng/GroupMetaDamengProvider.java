package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.GroupMetaMapper.GROUP_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.GroupMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class GroupMetaDamengProvider extends GroupMetaBaseSQLProvider {
  public String softDeleteGroupMetaByGroupId(@Param("groupId") Long groupId) {
    return "UPDATE "
        + GROUP_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE group_id = #{groupId} AND deleted_at = 0";
  }

  public String softDeleteGroupMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + GROUP_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }
}
