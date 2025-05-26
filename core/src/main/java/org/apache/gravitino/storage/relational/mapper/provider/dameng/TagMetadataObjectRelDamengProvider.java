package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.TagMetadataObjectRelBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class TagMetadataObjectRelDamengProvider extends TagMetadataObjectRelBaseSQLProvider {
  public String batchDeleteTagMetadataObjectRelsByTagIdsAndMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("tagIds") List<Long> tagIds) {
    return "<script>"
        + "UPDATE "
        + TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE tag_id IN "
        + "<foreach item='tagId' collection='tagIds' open='(' separator=',' close=')'>"
        + "#{tagId}"
        + "</foreach>"
        + " And metadata_object_id = #{metadataObjectId}"
        + " AND metadata_object_type = #{metadataObjectType} AND deleted_at = 0"
        + "</script>";
  }

  public String softDeleteTagMetadataObjectRelsByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return "UPDATE "
        + TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " te SET te.deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE te.tag_id IN (SELECT tm.tag_id FROM "
        + TagMetaMapper.TAG_TABLE_NAME
        + " tm WHERE tm.metalake_id IN (SELECT mm.metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND tm.tag_name = #{tagName} AND tm.deleted_at = 0) AND te.deleted_at = 0";
  }

  public String softDeleteTagMetadataObjectRelsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " te SET te.deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE EXISTS (SELECT * FROM "
        + TagMetaMapper.TAG_TABLE_NAME
        + " tm WHERE tm.metalake_id = #{metalakeId} AND tm.tag_id = te.tag_id"
        + " AND tm.deleted_at = 0) AND te.deleted_at = 0";
  }

  public String softDeleteTagMetadataObjectRelsByMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return " UPDATE "
        + TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metadata_object_id = #{metadataObjectId} AND deleted_at = 0"
        + " AND metadata_object_type = #{metadataObjectType}";
  }

  public String softDeleteTagMetadataObjectRelsByCatalogId(@Param("catalogId") Long catalogId) {
    return " UPDATE "
        + TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " tmt SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE tmt.deleted_at = 0 AND EXISTS ("
        + " SELECT ct.catalog_id FROM "
        + CatalogMetaMapper.TABLE_NAME
        + " ct WHERE ct.catalog_id = #{catalogId}  AND "
        + "ct.catalog_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'CATALOG'"
        + " UNION "
        + " SELECT st.catalog_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " st WHERE st.catalog_id = #{catalogId} AND "
        + "st.schema_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'SCHEMA'"
        + " UNION "
        + " SELECT tt.catalog_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " tt WHERE tt.catalog_id = #{catalogId} AND "
        + "tt.topic_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'TOPIC'"
        + " UNION "
        + " SELECT tat.catalog_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.catalog_id = #{catalogId} AND "
        + "tat.table_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'TABLE'"
        + " UNION "
        + " SELECT ft.catalog_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft WHERE ft.catalog_id = #{catalogId}  AND"
        + " ft.fileset_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'FILESET'"
        + " UNION "
        + " SELECT cot.catalog_id FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " cot WHERE cot.catalog_id = #{catalogId} AND"
        + " cot.column_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'COLUMN'"
        + " UNION "
        + " SELECT mt.catalog_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mt WHERE mt.catalog_id = #{catalogId} AND"
        + " mt.model_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'MODEL'"
        + ")";
  }

  public String softDeleteTagMetadataObjectRelsBySchemaId(@Param("schemaId") Long schemaId) {
    return " UPDATE "
        + TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " tmt SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE tmt.deleted_at = 0 AND EXISTS ("
        + " SELECT st.schema_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " st WHERE st.schema_id = #{schemaId} AND "
        + " st.schema_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'SCHEMA'"
        + " UNION "
        + " SELECT tt.schema_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " tt WHERE tt.schema_id = #{schemaId} AND "
        + "tt.topic_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'TOPIC'"
        + " UNION "
        + " SELECT tat.schema_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.schema_id = #{schemaId} AND "
        + "tat.table_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'TABLE'"
        + " UNION "
        + " SELECT ft.schema_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft WHERE ft.schema_id = #{schemaId} AND "
        + " ft.fileset_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'FILESET'"
        + " UNION "
        + " SELECT cot.schema_id FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " cot WHERE cot.schema_id = #{schemaId} AND "
        + " cot.column_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'COLUMN'"
        + " UNION "
        + " SELECT mt.schema_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mt WHERE mt.schema_id = #{schemaId} AND "
        + " mt.model_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'MODEL'"
        + ")";
  }

  public String softDeleteTagMetadataObjectRelsByTableId(@Param("tableId") Long tableId) {
    return " UPDATE "
        + TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " tmt SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE tmt.deleted_at = 0 AND EXISTS ("
        + " SELECT tat.table_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.table_id = #{tableId} AND "
        + " tat.table_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'TABLE'"
        + " UNION "
        + " SELECT cot.table_id FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " cot WHERE cot.table_id = #{tableId} AND"
        + " cot.column_id = tmt.metadata_object_id AND tmt.metadata_object_type = 'COLUMN'"
        + ")";
  }
}
