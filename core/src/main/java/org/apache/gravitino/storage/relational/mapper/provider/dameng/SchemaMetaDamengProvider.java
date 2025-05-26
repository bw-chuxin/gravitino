package org.apache.gravitino.storage.relational.mapper.provider.dameng;

import static org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.SchemaMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.ibatis.annotations.Param;

public class SchemaMetaDamengProvider extends SchemaMetaBaseSQLProvider {

  public String insertSchemaMetaOnDuplicateKeyUpdate(@Param("schemaMeta") SchemaPO schemaPO) {
    return "MERGE INTO "
        + TABLE_NAME
        + " as target "
        + " USING ("
        + " SELECT "
        + " #{schemaMeta.schemaId} AS schema_id, "
        + " #{schemaMeta.schemaName} AS schema_name,"
        + " #{schemaMeta.metalakeId} AS metalake_id,"
        + " #{schemaMeta.catalogId} AS catalog_id,"
        + " #{schemaMeta.schemaComment} AS schema_comment,"
        + " #{schemaMeta.properties} AS properties,"
        + " #{schemaMeta.auditInfo} AS audit_info,"
        + " #{schemaMeta.currentVersion} AS current_version,"
        + " #{schemaMeta.lastVersion} AS last_version,"
        + " #{schemaMeta.deletedAt} AS deleted_at"
        + "  FROM dual"
        + " ) source "
        + " ON (target.schema_id = source.schema_id) "
        + " WHEN MATCHED THEN "
        + " UPDATE SET "
        + " target.schema_name = source.schema_name,"
        + " target.metalake_id = source.metalake_id,"
        + " target.catalog_id = source.catalog_id,"
        + " target.schema_comment = source.schema_comment,"
        + " target.properties = source.properties,"
        + " target.audit_info = source.audit_info,"
        + " target.current_version = source.current_version,"
        + " target.last_version = source.last_version,"
        + " target.deleted_at = source.deleted_at"
        + " WHEN NOT MATCHED THEN"
        + " INSERT ("
        + " schema_id,"
        + " schema_name,"
        + " metalake_id,"
        + " catalog_id,"
        + " schema_comment,"
        + " properties,"
        + " audit_info,"
        + " current_version,"
        + " last_version,"
        + " deleted_at"
        + "  )"
        + " VALUES ("
        + " source.schema_id,"
        + " source.schema_name,"
        + " source.metalake_id,"
        + " source.catalog_id,"
        + " source.schema_comment,"
        + " source.properties,"
        + " source.audit_info,"
        + " source.current_version,"
        + " source.last_version,"
        + " source.deleted_at"
        + " )";
  }

  public String softDeleteSchemaMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String softDeleteSchemaMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteSchemaMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + TimestampUtil.unixTimestamp()
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }
}
