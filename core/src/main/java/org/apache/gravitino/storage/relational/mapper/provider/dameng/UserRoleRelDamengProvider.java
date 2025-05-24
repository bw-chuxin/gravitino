package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper.USER_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper.USER_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.provider.base.UserRoleRelBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class UserRoleRelDamengProvider extends UserRoleRelBaseSQLProvider {
  public String softDeleteUserRoleRelByUserId(@Param("userId") Long userId) {
    return "UPDATE "
        + USER_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE user_id = #{userId} AND deleted_at = 0";
  }

  public String softDeleteUserRoleRelByUserAndRoles(
      @Param("userId") Long userId, @Param("roleIds") List<Long> roleIds) {
    return "<script>"
        + "UPDATE "
        + USER_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE user_id = #{userId} AND role_id in ("
        + "<foreach collection='roleIds' item='roleId' separator=','>"
        + "#{roleId}"
        + "</foreach>"
        + ") "
        + "AND deleted_at = 0"
        + "</script>";
  }

  public String softDeleteUserRoleRelByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + USER_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE user_id IN (SELECT user_id FROM "
        + USER_TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0)"
        + " AND deleted_at = 0";
  }

  public String softDeleteUserRoleRelByRoleId(@Param("roleId") Long roleId) {
    return "UPDATE "
        + USER_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE role_id = #{roleId} AND deleted_at = 0";
  }

  public String deleteUserRoleRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + USER_ROLE_RELATION_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
