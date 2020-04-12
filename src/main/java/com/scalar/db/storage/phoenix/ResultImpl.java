package com.scalar.db.storage.phoenix;

import com.scalar.db.api.Result;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;

import javax.annotation.concurrent.Immutable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Immutable
public class ResultImpl implements Result {

  private final Map<String, Value> values;

  public ResultImpl (TableMetaData tableMetaData, List<String> projections, ResultSet resultSet)
    throws ExecutionException {
    values = new HashMap<>();

    if (projections.isEmpty()) {
      for (String projection : tableMetaData.getColumns()) {
        values.put(projection, getValue(tableMetaData, projection, resultSet));
      }
    } else {
      for (String projection : projections) {
        values.put(projection, getValue(tableMetaData, projection, resultSet));
      }
    }
  }

  private Value getValue(TableMetaData tableMetaData, String name, ResultSet resultSet)
    throws ExecutionException {

    Type type = tableMetaData.getColumnType(name);

    try {
      switch (type) {
        case BOOLEAN:
          return new BooleanValue(name, resultSet.getBoolean(name));

        case INT:
          return new IntValue(name, resultSet.getInt(name));

        case BIGINT:
          return new BigIntValue(name, resultSet.getLong(name));

        case FLOAT:
          return new FloatValue(name, resultSet.getFloat(name));

        case DOUBLE:
          return new DoubleValue(name, resultSet.getDouble(name));

        case TEXT:
          return new TextValue(name, resultSet.getString(name));

        case BLOB:
          return new BlobValue(name, resultSet.getBytes(name));

        default:
          throw new AssertionError();
      }
    } catch (SQLException e) {
      throw new ExecutionException("An error occurred", e);
    }
  }

  @Override
  public Optional<Key> getPartitionKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Key> getClusteringKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Value> getValue(String name) {
    return Optional.ofNullable(values.get(name));
  }

  @Override
  public Map<String, Value> getValues() {
    return Collections.unmodifiableMap(values);
  }
}
