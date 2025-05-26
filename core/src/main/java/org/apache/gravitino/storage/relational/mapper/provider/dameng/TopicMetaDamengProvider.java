package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.TopicMetaMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.TopicMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class TopicMetaDamengProvider extends TopicMetaBaseSQLProvider {
  public String softDeleteTopicMetasByTopicId(@Param("topicId") Long topicId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE topic_id = #{topicId} AND deleted_at = 0";
  }

  public String softDeleteTopicMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteTopicMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteTopicMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }
}
