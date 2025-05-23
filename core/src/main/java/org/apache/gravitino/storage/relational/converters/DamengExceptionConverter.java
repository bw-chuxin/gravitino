package org.apache.gravitino.storage.relational.converters;

import static org.apache.gravitino.storage.relational.converters.MySQLExceptionConverter.DUPLICATED_ENTRY_ERROR_CODE;

import java.io.IOException;
import java.sql.SQLException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;

/** Exception converter to Apache Gravitino exception for DM */
public class DamengExceptionConverter implements SQLExceptionConverter {
  @Override
  public void toGravitinoException(SQLException sqlException, Entity.EntityType type, String name)
      throws IOException {
    switch (sqlException.getErrorCode()) {
      case DUPLICATED_ENTRY_ERROR_CODE:
        throw new EntityAlreadyExistsException(
            sqlException, "The %s entity: %s already exists.", type.name(), name);
      default:
        throw new IOException(sqlException);
    }
  }
}
