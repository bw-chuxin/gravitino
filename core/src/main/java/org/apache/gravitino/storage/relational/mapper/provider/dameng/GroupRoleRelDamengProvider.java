package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.GroupRoleRelMapper.GROUP_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.GroupRoleRelMapper.GROUP_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.provider.base.GroupRoleRelBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class GroupRoleRelDamengProvider extends GroupRoleRelBaseSQLProvider {
  public String softDeleteGroupRoleRelByGroupId(@Param("groupId") Long groupId) {
    return "UPDATE "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE group_id = #{groupId} AND deleted_at = 0";
  }

  public String softDeleteGroupRoleRelByGroupAndRoles(
      @Param("groupId") Long groupId, @Param("roleIds") List<Long> roleIds) {
    return "<script>"
        + "UPDATE "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE group_id = #{groupId} AND role_id in ("
        + "<foreach collection='roleIds' item='roleId' separator=','>"
        + "#{roleId}"
        + "</foreach>"
        + ") "
        + "AND deleted_at = 0"
        + "</script>";
  }

  public String softDeleteGroupRoleRelByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE group_id IN (SELECT group_id FROM "
        + GROUP_TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0)"
        + " AND deleted_at = 0";
  }

  public String softDeleteGroupRoleRelByRoleId(Long roleId) {
    return "UPDATE "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE role_id = #{roleId} AND deleted_at = 0";
  }
}
