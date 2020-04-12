package com.scalar.db.storage.phoenix;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.exception.storage.ExecutionException;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;

public class TableMetaDataManager {

  private final LoadingCache<TableName, TableMetaData> tableMetaDataCache;

  public TableMetaDataManager(PhoenixConnection phoenixConnection) {
    Objects.requireNonNull(phoenixConnection);

    // TODO Add expiration for alter table
    tableMetaDataCache = CacheBuilder.newBuilder()
      .build(new CacheLoader<TableName, TableMetaData>() {
        @Override
        public TableMetaData load(@Nonnull TableName tableName) throws Exception {
          try (Connection connection = phoenixConnection.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();

            // Get types of the columns
            ImmutableMap.Builder<String, Type> columnTypesBuilder = ImmutableMap.builder();
            try (ResultSet columns = metaData.getColumns(null, tableName.getSchema(),
              tableName.getTable(), null)) {
              while (columns.next()) {
                columnTypesBuilder.put(columns.getString("COLUMN_NAME"),
                  convertType(columns.getString("DATA_TYPE")));
              }
            }

            // Get the primary keys
            ImmutableList.Builder<String> primaryKeysBuilder = ImmutableList.builder();
            try (ResultSet primaryKeys = metaData.getPrimaryKeys(null, tableName.getSchema(),
              tableName.getTable())) {
              while (primaryKeys.next()) {
                primaryKeysBuilder.add(primaryKeys.getString("COLUMN_NAME"));
              }
            }

            // Get the dummy column family
            byte[] dummyColumnFamily = null;
            try (PreparedStatement preparedStatement = connection.prepareStatement(
              "SELECT COLUMN_FAMILY FROM SYSTEM.CATALOG WHERE TABLE_SCHEM=? AND TABLE_NAME=?" +
                " AND COLUMN_NAME IS NOT NULL AND COLUMN_FAMILY IS NOT NULL" +
                " ORDER BY ORDINAL_POSITION LIMIT 1")) {
              preparedStatement.setString(1, tableName.getSchema());
              preparedStatement.setString(2, tableName.getTable());
              try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                  dummyColumnFamily = Bytes.toBytes(resultSet.getString("COLUMN_FAMILY"));
                }
              }
            }

            // Get whether the table is column encoded or not
            boolean columnEncoded = false;
            try (PreparedStatement preparedStatement = connection.prepareStatement(
              "SELECT ENCODING_SCHEME FROM SYSTEM.CATALOG where TABLE_SCHEM=? AND TABLE_NAME=?" +
                " AND ENCODING_SCHEME IS NOT NULL")) {
              preparedStatement.setString(1, tableName.getSchema());
              preparedStatement.setString(2, tableName.getTable());
              try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                  columnEncoded = resultSet.getInt("ENCODING_SCHEME") != 0;
                }
              }
            }

            return new TableMetaData(tableName, columnTypesBuilder.build(),
              primaryKeysBuilder.build(), dummyColumnFamily, columnEncoded);
          }
        }
      });
  }

  private Type convertType(String dataType) {
    switch (dataType) {
      case "16": // BOOLEAN
        return Type.BOOLEAN;

      case "4": // INTEGER
        return Type.INT;

      case "-5": // BIGINT
        return Type.BIGINT;

      case "6": // FLOAT
        return Type.FLOAT;

      case "8": // DOUBLE
        return Type.DOUBLE;

      case "12": // VARCHAR
        return Type.TEXT;

      case "-3": // VARBINARY
        return Type.BLOB;

      default:
        throw new IllegalArgumentException("Unsupported type :" + dataType);
    }
  }

  public TableMetaData getTableMetaData(TableName tableName) throws ExecutionException {
    try {
      return tableMetaDataCache.get(tableName);
    } catch (java.util.concurrent.ExecutionException e) {
      throw new ExecutionException("An error occurred during getting a table meta data.", e);
    }
  }

  public void clearTableMetaDataCache() {
    tableMetaDataCache.invalidateAll();
  }
}
