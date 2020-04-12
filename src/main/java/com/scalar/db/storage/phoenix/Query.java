package com.scalar.db.storage.phoenix;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
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
import com.scalar.db.io.ValueVisitor;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class Query {

  public class SelectQueryBuilder {
    private final List<String> projections;
    private TableName tableName;
    private Key partitionKey;
    private Key clusteringKey;
    private Key commonClusteringKey;
    private Value startValue;
    private boolean startInclusive;
    private Value endValue;
    private boolean endInclusive;
    private int limit;
    private List<Scan.Ordering> orderings = Collections.emptyList();

    private SelectQueryBuilder(List<String> projections) {
      this.projections = Objects.requireNonNull(projections);
    }

    public SelectQueryBuilder from(TableName tableName) {
      this.tableName = tableName;
      return this;
    }

    public SelectQueryBuilder where(Key partitionKey, @Nullable Key clusteringKey) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      return this;
    }

    public SelectQueryBuilder where(Key partitionKey, @Nullable Key startClusteringKey,
      boolean startInclusive, @Nullable Key endClusteringKey, boolean endInclusive) {
      this.partitionKey = partitionKey;

      if (startClusteringKey != null) {
        commonClusteringKey = new Key(startClusteringKey.get()
          .subList(0, startClusteringKey.size() - 1));
      } else if (endClusteringKey != null) {
        commonClusteringKey = new Key(endClusteringKey.get()
          .subList(0, endClusteringKey.size() - 1));
      }

      if (startClusteringKey != null) {
        startValue = startClusteringKey.get().get(startClusteringKey.size() - 1);
        this.startInclusive = startInclusive;
      }

      if (endClusteringKey != null) {
        endValue = endClusteringKey.get().get(endClusteringKey.size() - 1);
        this.endInclusive = endInclusive;
      }
      return this;
    }

    public SelectQueryBuilder limit(int limit) {
      this.limit = limit;
      return this;
    }

    public SelectQueryBuilder orderBy(List<Scan.Ordering> orderings) {
      this.orderings = orderings;
      return this;
    }

    public SelectQuery build() {
      if (tableName == null || partitionKey == null) {
        throw new IllegalStateException("tableName or partitionKey is null.");
      }
      return new SelectQuery(projections, tableName, partitionKey, clusteringKey,
        commonClusteringKey, startValue, startInclusive, endValue, endInclusive, limit,
        orderings);
    }
  }

  public class SelectQuery {
    private final List<String> projections;
    private final TableName tableName;
    private final Key partitionKey;
    @Nullable private final Key clusteringKey;
    @Nullable private final Key commonClusteringKey;
    @Nullable private final Value startValue;
    private final boolean startInclusive;
    @Nullable private final Value endValue;
    private final boolean endInclusive;
    private final int limit;
    private final List<Scan.Ordering> orderings;

    public SelectQuery(List<String> projections, TableName tableName, Key partitionKey,
      @Nullable Key clusteringKey, @Nullable Key commonClusteringKey, @Nullable Value startValue,
      boolean startInclusive, @Nullable Value endValue, boolean endInclusive, int limit,
      List<Scan.Ordering> orderings) {

      this.projections = projections;
      this.tableName = tableName;
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      this.commonClusteringKey = commonClusteringKey;
      this.startValue = startValue;
      this.startInclusive = startInclusive;
      this.endValue = endValue;
      this.endInclusive = endInclusive;
      this.limit = limit;
      this.orderings = orderings;
    }

    private String sql() {
      return "SELECT " + projectionSQLString() + " FROM " + tableName + " WHERE " +
        conditionSQLString() + orderBySQLString() + limitSQLString();
    }

    private String projectionSQLString() {
      if (projections.isEmpty()) {
        return "*";
      }
      return projections.stream().map(p -> "\"" + p + "\"").collect(Collectors.joining(","));
    }

    private String conditionSQLString() {
      List<String> conditions = new ArrayList<>();

      for (Value value : partitionKey) {
        conditions.add("\"" + value.getName() + "\"=?");
      }

      if (clusteringKey != null) {
        for (Value value : clusteringKey) {
          conditions.add("\"" + value.getName() + "\"=?");
        }
      }

      if (commonClusteringKey != null) {
        for (Value value : commonClusteringKey) {
          conditions.add("\"" + value.getName() + "\"=?");
        }
      }

      if (startValue != null) {
        conditions.add("\"" + startValue.getName() + "\"" + (startInclusive ? ">=?" : ">?"));
      }

      if (endValue != null) {
        conditions.add("\"" + endValue.getName() + "\"" + (endInclusive ? "<=?" : "<?"));
      }

      return String.join(" AND ", conditions);
    }

    private String limitSQLString() {
      if (limit > 0) {
        return " LIMIT " + limit;
      }
      return "";
    }

    private String orderBySQLString() {
      if (orderings.isEmpty()) {
        return "";
      }

      return " ORDER BY " + orderings.stream()
        .map(o -> "\"" + o.getName() + "\" " + o.getOrder())
        .collect(Collectors.joining(","));
    }

    public PreparedStatement prepare(Connection connection) throws ExecutionException {
      try {
        return connection.prepareStatement(sql());
      } catch (SQLException e) {
        throw new ExecutionException("An error occurred in prepare()", e);
      }
    }

    public void bind(PreparedStatement preparedStatement) throws ExecutionException {
      PreparedStatementBinder preparedStatementBinder =
        new PreparedStatementBinder(preparedStatement);

      for (Value value : partitionKey) {
        value.accept(preparedStatementBinder);
        if (preparedStatementBinder.hasSQLException()) {
          throw new ExecutionException("An error occurred in bind()",
            preparedStatementBinder.getSQLException());
        }
      }

      if (clusteringKey != null) {
        for (Value value : clusteringKey) {
          value.accept(preparedStatementBinder);
          if (preparedStatementBinder.hasSQLException()) {
            throw new ExecutionException("An error occurred in bind()",
              preparedStatementBinder.getSQLException());
          }
        }
      }

      if (commonClusteringKey != null) {
        for (Value value : commonClusteringKey) {
          value.accept(preparedStatementBinder);
          if (preparedStatementBinder.hasSQLException()) {
            throw new ExecutionException("An error occurred in bind()",
              preparedStatementBinder.getSQLException());
          }
        }
      }

      if (startValue != null) {
        startValue.accept(preparedStatementBinder);
        if (preparedStatementBinder.hasSQLException()) {
          throw new ExecutionException("An error occurred in bind()",
            preparedStatementBinder.getSQLException());
        }
      }

      if (endValue != null) {
        endValue.accept(preparedStatementBinder);
        if (preparedStatementBinder.hasSQLException()) {
          throw new ExecutionException("An error occurred in bind()",
            preparedStatementBinder.getSQLException());
        }
      }
    }

    public Result getResult(ResultSet resultSet) throws ExecutionException {
      return new ResultImpl(tableMetaDataManager.getTableMetaData(tableName), projections,
        resultSet);
    }

    @Override
    public String toString() {
      return sql();
    }
  }

  public static class UpsertQueryBuilder {
    private TableName tableName;
    private Key partitionKey;
    private Key clusteringKey;
    private Map<String, Value> values;

    private UpsertQueryBuilder() {
    }

    public UpsertQueryBuilder into(TableName tableName) {
      this.tableName = tableName;
      return this;
    }

    public UpsertQueryBuilder values(Key partitionKey, @Nullable Key clusteringKey,
      Map<String, Value> values) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      this.values = values;
      return this;
    }

    public UpsertQuery build() {
      if (tableName == null || partitionKey == null) {
        throw new IllegalStateException("tableName or partitionKey is null.");
      }

      return new UpsertQuery(tableName, partitionKey, clusteringKey, values);
    }
  }

  public static class UpsertQuery {
    private final TableName tableName;
    private final Key partitionKey;
    private @Nullable Key clusteringKey;
    private final Map<String, Value> values;

    public UpsertQuery(TableName tableName, Key partitionKey, @Nullable Key clusteringKey,
      Map<String, Value> values) {
      this.tableName = tableName;
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      this.values = values;
    }

    private String sql() {
      return "UPSERT INTO " + tableName + " " + makeValuesSQLString();
    }

    private String makeValuesSQLString() {
      List<String> names = new ArrayList<>();

      for (Value value : partitionKey) {
        names.add(value.getName());
      }

      if (clusteringKey != null) {
        for (Value value : clusteringKey) {
          names.add(value.getName());
        }
      }

      names.addAll(values.keySet());

      return "(" + names.stream().map(n -> "\"" + n + "\"").collect(Collectors.joining(",")) + ")"
        + " VALUES(" + names.stream().map(n -> "?").collect(Collectors.joining(",")) + ")";
    }

    public PreparedStatement prepare(Connection connection) throws ExecutionException {
      try {
        return connection.prepareStatement(sql());
      } catch (SQLException e) {
        throw new ExecutionException("An error occurred in prepare()", e);
      }
    }

    public void bind(PreparedStatement preparedStatement) throws ExecutionException {
      PreparedStatementBinder preparedStatementBinder =
        new PreparedStatementBinder(preparedStatement);

      for (Value value : partitionKey) {
        value.accept(preparedStatementBinder);
        if (preparedStatementBinder.hasSQLException()) {
          throw new ExecutionException("An error occurred in bind()",
            preparedStatementBinder.getSQLException());
        }
      }

      if (clusteringKey != null) {
        for (Value value : clusteringKey) {
          value.accept(preparedStatementBinder);
          if (preparedStatementBinder.hasSQLException()) {
            throw new ExecutionException("An error occurred in bind()",
              preparedStatementBinder.getSQLException());
          }
        }
      }

      for (Value value : values.values()) {
        value.accept(preparedStatementBinder);
        if (preparedStatementBinder.hasSQLException()) {
          throw new ExecutionException("An error occurred in bind()",
            preparedStatementBinder.getSQLException());
        }
      }
    }

    @Override
    public String toString() {
      return sql();
    }
  }

  public static class DeleteQueryBuilder {
    private TableName tableName;
    private Key partitionKey;
    private Key clusteringKey;

    private DeleteQueryBuilder() {
    }

    public DeleteQueryBuilder from(TableName tableName) {
      this.tableName = tableName;
      return this;
    }

    public DeleteQueryBuilder where(Key partitionKey, @Nullable Key clusteringKey) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
      return this;
    }

    public DeleteQuery build() {
      if (tableName == null || partitionKey == null) {
        throw new IllegalStateException("tableName or partitionKey is null.");
      }
      return new DeleteQuery(tableName, partitionKey, clusteringKey);
    }
  }

  public static class DeleteQuery {
    private final TableName tableName;
    private final Key partitionKey;
    @Nullable private final Key clusteringKey;

    public DeleteQuery(TableName tableName, Key partitionKey, @Nullable Key clusteringKey) {
      this.tableName = tableName;
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
    }

    private String sql() {
      return "DELETE FROM " + tableName + " WHERE " + conditionSQLString();
    }

    private String conditionSQLString() {
      List<String> conditions = new ArrayList<>();

      for (Value value : partitionKey) {
        conditions.add("\"" + value.getName() + "\"=?");
      }

      if (clusteringKey != null) {
        for (Value value : clusteringKey) {
          conditions.add("\"" + value.getName() + "\"=?");
        }
      }

      return String.join(" AND ", conditions);
    }

    public PreparedStatement prepare(Connection connection) throws ExecutionException {
      try {
        return connection.prepareStatement(sql());
      } catch (SQLException e) {
        throw new ExecutionException("An error occurred in prepare()", e);
      }
    }

    public void bind(PreparedStatement preparedStatement) throws ExecutionException {
      PreparedStatementBinder preparedStatementBinder =
        new PreparedStatementBinder(preparedStatement);

      for (Value value : partitionKey) {
        value.accept(preparedStatementBinder);
        if (preparedStatementBinder.hasSQLException()) {
          throw new ExecutionException("An error occurred in bind()",
            preparedStatementBinder.getSQLException());
        }
      }

      if (clusteringKey != null) {
        for (Value value : clusteringKey) {
          value.accept(preparedStatementBinder);
          if (preparedStatementBinder.hasSQLException()) {
            throw new ExecutionException("An error occurred in bind()",
              preparedStatementBinder.getSQLException());
          }
        }
      }
    }

    @Override
    public String toString() {
      return sql();
    }
  }

  private static class PreparedStatementBinder implements ValueVisitor {
    private final PreparedStatement preparedStatement;
    private int index = 1;
    private SQLException sqlException;

    public PreparedStatementBinder(PreparedStatement preparedStatement) {
      this.preparedStatement = preparedStatement;
    }

    public boolean hasSQLException() {
      return sqlException != null;
    }

    public SQLException getSQLException() {
      return sqlException;
    }

    @Override
    public void visit(BooleanValue value) {
      try {
        preparedStatement.setBoolean(index++, value.get());
      } catch (SQLException e) {
        sqlException = e;
      }
    }

    @Override
    public void visit(IntValue value) {
      try {
        preparedStatement.setInt(index++, value.get());
      } catch (SQLException e) {
        sqlException = e;
      }
    }

    @Override
    public void visit(BigIntValue value) {
      try {
        preparedStatement.setLong(index++, value.get());
      } catch (SQLException e) {
        sqlException = e;
      }
    }

    @Override
    public void visit(FloatValue value) {
      try {
        preparedStatement.setFloat(index++, value.get());
      } catch (SQLException e) {
        sqlException = e;
      }
    }

    @Override
    public void visit(DoubleValue value) {
      try {
        preparedStatement.setDouble(index++, value.get());
      } catch (SQLException e) {
        sqlException = e;
      }
    }

    @Override
    public void visit(TextValue value) {
      try {
        preparedStatement.setString(index++, value.getString().orElse(null));
      } catch (SQLException e) {
        sqlException = e;
      }
    }

    @Override
    public void visit(BlobValue value) {
      try {
        preparedStatement.setBytes(index++, value.get().orElse(null));
      } catch (SQLException e) {
        sqlException = e;
      }
    }
  }

  private final TableMetaDataManager tableMetaDataManager;

  public Query(TableMetaDataManager tableMetaDataManager) {
    this.tableMetaDataManager = Objects.requireNonNull(tableMetaDataManager);
  }

  public SelectQueryBuilder select(List<String> projections) {
    return new SelectQueryBuilder(projections);
  }

  public UpsertQueryBuilder upsert() {
    return new UpsertQueryBuilder();
  }

  public DeleteQueryBuilder delete() {
    return new DeleteQueryBuilder();
  }
}
