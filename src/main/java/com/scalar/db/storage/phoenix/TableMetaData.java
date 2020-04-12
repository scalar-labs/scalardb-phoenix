package com.scalar.db.storage.phoenix;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Objects;


public class TableMetaData {

  private final TableName tableName;
  private final ImmutableMap<String, Type> columnTypes;
  private final ImmutableList<String> primaryKeys;
  private final ImmutableList<String> columns;
  private final byte[] dummyColumnFamily;
  private final boolean columnEncoded;

  public TableMetaData(TableName tableName, ImmutableMap<String, Type> columnTypes,
    ImmutableList<String> primaryKeys, byte[] dummyColumnFamily, boolean columnEncoded) {
    this.tableName = Objects.requireNonNull(tableName);
    this.columnTypes = Objects.requireNonNull(columnTypes);
    this.primaryKeys = Objects.requireNonNull(primaryKeys);
    columns = ImmutableList.copyOf(columnTypes.keySet());
    this.dummyColumnFamily = dummyColumnFamily;
    this.columnEncoded = columnEncoded;
  }

  public TableName getTableName() {
    return tableName;
  }

  public List<String> getPrimaryKeys() {
    return primaryKeys;
  }

  public List<String> getColumns() {
    return columns;
  }

  public Type getColumnType(String name) {
    return columnTypes.get(name);
  }

  public boolean columnExists(String name) {
    return columnTypes.containsKey(name);
  }

  public byte[] getDummyColumnFamily() {
    return dummyColumnFamily;
  }

  public boolean isColumnEncoded() {
    return columnEncoded;
  }
}
