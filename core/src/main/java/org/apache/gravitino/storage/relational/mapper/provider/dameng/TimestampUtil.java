package org.apache.gravitino.storage.relational.mapper.provider.dameng;

public final class TimestampUtil {
  public static final String unixTimestamp() {
    return "UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000";
  }
}
