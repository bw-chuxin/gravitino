package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.ROLE_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.RoleMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class RoleMetaDamengProvider extends RoleMetaBaseSQLProvider {
  public String softDeleteRoleMetaByRoleId(Long roleId) {
    return "UPDATE "
        + ROLE_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE role_id = #{roleId} AND deleted_at = 0";
  }

  public String softDeleteRoleMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + ROLE_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }
}
