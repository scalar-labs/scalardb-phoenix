package com.scalar.db.storage.phoenix;

import java.util.Objects;

public class TableName {
  private final String schema;
  private final String table;

  public TableName(String schema, String table) {
    if (schema == null || table == null) {
      throw new IllegalArgumentException();
    }
    this.schema = schema;
    this.table = table;
  }

  public String getSchema() {
    return schema;
  }

  public String getTable() {
    return table;
  }

  @Override
  public String toString() {
    return "\"" + schema + "\".\"" + table + "\"";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TableName tableName = (TableName) o;
    return schema.equals(tableName.schema) &&
      table.equals(tableName.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, table);
  }
}
