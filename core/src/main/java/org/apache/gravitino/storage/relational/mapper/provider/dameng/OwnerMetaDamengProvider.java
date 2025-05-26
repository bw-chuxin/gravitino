package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper.OWNER_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.OwnerMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class OwnerMetaDamengProvider extends OwnerMetaBaseSQLProvider {
  public String softDeleteOwnerRelByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return "UPDATE "
        + OWNER_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metadata_object_id = #{metadataObjectId} AND metadata_object_type = #{metadataObjectType} AND deleted_at = 0";
  }

  public String softDeleteOwnerRelByOwnerIdAndType(
      @Param("ownerId") Long ownerId, @Param("ownerType") String ownerType) {
    return "UPDATE "
        + OWNER_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE owner_id = #{ownerId} AND owner_type = #{ownerType} AND deleted_at = 0";
  }

  public String softDeleteOwnerRelByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE  "
        + OWNER_TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at =0";
  }

  public String softDeleteOwnerRelByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE  "
        + OWNER_TABLE_NAME
        + " ot SET ot.deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE ot.deleted_at = 0 AND EXISTS ("
        + " SELECT ct.catalog_id FROM "
        + CatalogMetaMapper.TABLE_NAME
        + " ct WHERE ct.catalog_id = #{catalogId} AND "
        + " ct.catalog_id = ot.metadata_object_id AND ot.metadata_object_type = 'CATALOG'"
        + " UNION "
        + " SELECT st.catalog_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " st WHERE st.catalog_id = #{catalogId} AND "
        + " st.schema_id = ot.metadata_object_id AND ot.metadata_object_type = 'SCHEMA'"
        + " UNION "
        + " SELECT tt.catalog_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " tt WHERE tt.catalog_id = #{catalogId} AND "
        + " tt.topic_id = ot.metadata_object_id AND ot.metadata_object_type = 'TOPIC'"
        + " UNION "
        + " SELECT tat.catalog_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.catalog_id = #{catalogId} AND "
        + " tat.table_id = ot.metadata_object_id AND ot.metadata_object_type = 'TABLE'"
        + " UNION "
        + " SELECT ft.catalog_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft WHERE ft.catalog_id = #{catalogId} AND"
        + " ft.fileset_id = ot.metadata_object_id AND ot.metadata_object_type = 'FILESET'"
        + " UNION "
        + " SELECT mt.catalog_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mt WHERE mt.catalog_id = #{catalogId} AND"
        + " mt.model_id = ot.metadata_object_id AND ot.metadata_object_type = 'MODEL'"
        + ")";
  }

  public String softDeleteOwnerRelBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE  "
        + OWNER_TABLE_NAME
        + " ot SET ot.deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE ot.deleted_at = 0 AND EXISTS ("
        + " SELECT st.schema_id FROM "
        + SchemaMetaMapper.TABLE_NAME
        + " st WHERE st.schema_id = #{schemaId} AND"
        + " st.schema_id = ot.metadata_object_id AND ot.metadata_object_type = 'SCHEMA'"
        + " UNION "
        + " SELECT tt.schema_id FROM "
        + TopicMetaMapper.TABLE_NAME
        + " tt WHERE tt.schema_id = #{schemaId} AND "
        + " tt.topic_id = ot.metadata_object_id AND ot.metadata_object_type = 'TOPIC'"
        + " UNION "
        + " SELECT tat.schema_id FROM "
        + TableMetaMapper.TABLE_NAME
        + " tat WHERE tat.schema_id = #{schemaId} AND "
        + " tat.table_id = ot.metadata_object_id AND ot.metadata_object_type = 'TABLE'"
        + " UNION "
        + " SELECT ft.schema_id FROM "
        + FilesetMetaMapper.META_TABLE_NAME
        + " ft WHERE ft.schema_id = #{schemaId} AND "
        + " ft.fileset_id = ot.metadata_object_id AND ot.metadata_object_type = 'FILESET'"
        + " UNION "
        + " SELECT mt.schema_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mt WHERE mt.schema_id = #{schemaId} AND "
        + " mt.model_id = ot.metadata_object_id AND ot.metadata_object_type = 'MODEL'"
        + ")";
  }
}
