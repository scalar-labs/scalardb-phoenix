package com.scalar.db.storage.phoenix;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationConditionVisitor;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ConditionalUpdater implements MutationConditionVisitor {

  private static final byte[] DUMMY_COLUMN_QUALIFIER = Bytes.toBytes("_0");
  private static final byte[] DUMMY_COLUMN_QUALIFIER_IN_COLUMN_ENCODED = Bytes.toBytes(0);
  private static final byte[] DUMMY_COLUMN_VALUE = Bytes.toBytes("x");

  private final PhoenixConnection phoenixConnection;
  private final Query query;
  private final TableMetaData tableMetaData;
  private final Key partitionKey;
  private final Key clusteringKey;
  private final Mutation mutation;

  private boolean result;
  private ExecutionException exception;

  public ConditionalUpdater(PhoenixConnection phoenixConnection, Query query,
    TableMetaData tableMetaData, Key partitionKey, @Nullable Key clusteringKey,
    Mutation mutation) {
    this.phoenixConnection = Objects.requireNonNull(phoenixConnection);
    this.query = Objects.requireNonNull(query);
    this.tableMetaData = tableMetaData;
    this.partitionKey = Objects.requireNonNull(partitionKey);
    this.clusteringKey = clusteringKey;
    this.mutation = Objects.requireNonNull(mutation);
  }

  @Override
  public void visit(PutIf condition) {
    if (condition.getExpressions().size() > 1) {
      throw new UnsupportedOperationException("Multiple conditions are not supported.");
    }

    Put put = (Put) mutation;
    ConditionalExpression expression = condition.getExpressions().get(0);
    try {
      Cell conditionCell = getConditionCell(expression);

      Pair<byte[], org.apache.hadoop.hbase.client.Put> hbaseTableNameAndPut =
        getHBaseTableNameAndPut(put.getValues());
      byte[] hbaseTableName = hbaseTableNameAndPut.getFirst();
      org.apache.hadoop.hbase.client.Put hbasePut = hbaseTableNameAndPut.getSecond();

      try (Connection connection = phoenixConnection.getConnectionWithoutAutoCommit()) {
        org.apache.phoenix.jdbc.PhoenixConnection pcon =
          (org.apache.phoenix.jdbc.PhoenixConnection) connection;
        try (Table table = pcon.getQueryServices().getTable(hbaseTableName)) {
          result = table.checkAndMutate(hbasePut.getRow(), CellUtil.cloneFamily(conditionCell))
            .qualifier(CellUtil.cloneQualifier(conditionCell))
            .ifMatches(toCompareOperator(expression.getOperator()),
              CellUtil.cloneValue(conditionCell))
            .thenPut(hbasePut);
        }
      }
    } catch (ExecutionException e) {
      exception = e;
    } catch (SQLException | IOException e) {
      exception = new ExecutionException("An error occurred in conditional update", e);
    }
  }

  @Override
  public void visit(PutIfExists condition) {
    Put put = (Put) mutation;

    try {
      Pair<byte[], org.apache.hadoop.hbase.client.Put> hbaseTableNameAndPut =
        getHBaseTableNameAndPut(put.getValues());
      byte[] hbaseTableName = hbaseTableNameAndPut.getFirst();
      org.apache.hadoop.hbase.client.Put hbasePut = hbaseTableNameAndPut.getSecond();

      try (Connection connection = phoenixConnection.getConnectionWithoutAutoCommit()) {
        org.apache.phoenix.jdbc.PhoenixConnection pcon =
          (org.apache.phoenix.jdbc.PhoenixConnection) connection;
        try (Table table = pcon.getQueryServices().getTable(hbaseTableName)) {
          result = table.checkAndMutate(hbasePut.getRow(), tableMetaData.getDummyColumnFamily())
            .qualifier(getDummyQualifier())
            .ifEquals(DUMMY_COLUMN_VALUE)
            .thenPut(hbasePut);
        }
      }
    } catch (ExecutionException e) {
      exception = e;
    } catch (SQLException | IOException e) {
      exception = new ExecutionException("An error occurred in conditional update", e);
    }
  }

  @Override
  public void visit(PutIfNotExists condition) {
    Put put = (Put) mutation;

    try {
      Pair<byte[], org.apache.hadoop.hbase.client.Put> hbaseTableNameAndPut =
        getHBaseTableNameAndPut(put.getValues());
      byte[] hbaseTableName = hbaseTableNameAndPut.getFirst();
      org.apache.hadoop.hbase.client.Put hbasePut = hbaseTableNameAndPut.getSecond();

      try (Connection connection = phoenixConnection.getConnectionWithoutAutoCommit()) {
        org.apache.phoenix.jdbc.PhoenixConnection pcon =
          (org.apache.phoenix.jdbc.PhoenixConnection) connection;
        try (Table table = pcon.getQueryServices().getTable(hbaseTableName)) {
          result = table.checkAndMutate(hbasePut.getRow(), tableMetaData.getDummyColumnFamily())
            .qualifier(getDummyQualifier())
            .ifNotExists()
            .thenPut(hbasePut);
        }
      }
    } catch (ExecutionException e) {
      exception = e;
    } catch (SQLException | IOException e) {
      exception = new ExecutionException("An error occurred in conditional update", e);
    }
  }

  @Override
  public void visit(DeleteIf condition) {
    if (condition.getExpressions().size() > 1) {
      throw new UnsupportedOperationException("Multiple conditions are not supported.");
    }

    ConditionalExpression expression = condition.getExpressions().get(0);
    try {
      Cell conditionCell = getConditionCell(expression);

      Pair<byte[], org.apache.hadoop.hbase.client.Delete> hbaseTableNameAndDelete =
        getHBaseTableNameAndDelete();
      byte[] hbaseTableName = hbaseTableNameAndDelete.getFirst();
      org.apache.hadoop.hbase.client.Delete hbaseDelete = hbaseTableNameAndDelete.getSecond();

      try (Connection connection = phoenixConnection.getConnectionWithoutAutoCommit()) {
        org.apache.phoenix.jdbc.PhoenixConnection pcon =
          (org.apache.phoenix.jdbc.PhoenixConnection) connection;
        try (Table table = pcon.getQueryServices().getTable(hbaseTableName)) {
          result = table.checkAndMutate(hbaseDelete.getRow(), CellUtil.cloneFamily(conditionCell))
            .qualifier(CellUtil.cloneQualifier(conditionCell))
            .ifMatches(toCompareOperator(expression.getOperator()),
              CellUtil.cloneValue(conditionCell))
            .thenDelete(hbaseDelete);
        }
      }
    } catch (ExecutionException e) {
      exception = e;
    } catch (SQLException | IOException e) {
      exception = new ExecutionException("An error occurred in conditional update", e);
    }
  }

  @Override
  public void visit(DeleteIfExists condition) {
    try {
      Pair<byte[], org.apache.hadoop.hbase.client.Delete> hbaseTableNameAndDelete =
        getHBaseTableNameAndDelete();
      byte[] hbaseTableName = hbaseTableNameAndDelete.getFirst();
      org.apache.hadoop.hbase.client.Delete hbaseDelete = hbaseTableNameAndDelete.getSecond();

      try (Connection connection = phoenixConnection.getConnectionWithoutAutoCommit()) {
        org.apache.phoenix.jdbc.PhoenixConnection pcon =
          (org.apache.phoenix.jdbc.PhoenixConnection) connection;
        try (Table table = pcon.getQueryServices().getTable(hbaseTableName)) {
          result = table.checkAndMutate(hbaseDelete.getRow(),
            tableMetaData.getDummyColumnFamily())
            .qualifier(getDummyQualifier())
            .ifEquals(DUMMY_COLUMN_VALUE)
            .thenDelete(hbaseDelete);
        }
      }
    } catch (ExecutionException e) {
      exception = e;
    } catch (SQLException | IOException e) {
      exception = new ExecutionException("An error occurred in conditional update", e);
    }
  }

  private Pair<byte[], org.apache.hadoop.hbase.client.Put> getHBaseTableNameAndPut(
    String name, Value value) throws SQLException, ExecutionException {
    return getHBaseTableNameAndPut(Collections.singletonMap(name, value));
  }

  private Pair<byte[], org.apache.hadoop.hbase.client.Put> getHBaseTableNameAndPut(
    Map<String, Value> values) throws SQLException, ExecutionException {

    Query.UpsertQuery upsertQuery = query.upsert().into(tableMetaData.getTableName())
      .values(partitionKey, clusteringKey, values).build();

    try (Connection connection = phoenixConnection.getConnectionWithoutAutoCommit()) {
      PreparedStatement preparedStatement = upsertQuery.prepare(connection);
      upsertQuery.bind(preparedStatement);
      preparedStatement.executeUpdate();

      org.apache.phoenix.jdbc.PhoenixConnection pcon =
        (org.apache.phoenix.jdbc.PhoenixConnection) connection;

      final Iterator<Pair<byte[], List<org.apache.hadoop.hbase.client.Mutation>>> iterator =
        pcon.getMutationState().toMutations(false);

      byte[] tableName = null;
      org.apache.hadoop.hbase.client.Put hbasePut = null;
      while (iterator.hasNext()) {
        Pair<byte[], List<org.apache.hadoop.hbase.client.Mutation>> kvPair = iterator.next();
        tableName = kvPair.getFirst();
        hbasePut = (org.apache.hadoop.hbase.client.Put) kvPair.getSecond().get(0);
      }
      connection.rollback();

      if (tableName == null && hbasePut == null) {
        throw new ExecutionException("Can't get the table name and the HBase Put");
      }
      return new Pair<>(tableName, hbasePut);
    }
  }

  private Pair<byte[], org.apache.hadoop.hbase.client.Delete> getHBaseTableNameAndDelete()
    throws SQLException, ExecutionException {

    Query.DeleteQuery deleteQuery = query.delete().from(tableMetaData.getTableName())
      .where(partitionKey, clusteringKey).build();

    try (Connection connection = phoenixConnection.getConnectionWithoutAutoCommit()) {
      PreparedStatement preparedStatement = deleteQuery.prepare(connection);
      deleteQuery.bind(preparedStatement);
      preparedStatement.executeUpdate();

      org.apache.phoenix.jdbc.PhoenixConnection pcon =
        (org.apache.phoenix.jdbc.PhoenixConnection) connection;

      final Iterator<Pair<byte[], List<org.apache.hadoop.hbase.client.Mutation>>> iterator =
        pcon.getMutationState().toMutations(false);

      byte[] tableName = null;
      org.apache.hadoop.hbase.client.Delete hbaseDelete = null;
      if (iterator.hasNext()) {
        Pair<byte[], List<org.apache.hadoop.hbase.client.Mutation>> kvPair = iterator.next();
        tableName = kvPair.getFirst();
        hbaseDelete = (org.apache.hadoop.hbase.client.Delete) kvPair.getSecond().get(0);
      }
      connection.rollback();

      if (tableName == null && hbaseDelete == null) {
        throw new ExecutionException("Can't get the table name and the HBase Delete");
      }
      return new Pair<>(tableName, hbaseDelete);
    }
  }

  private Cell getConditionCell(ConditionalExpression expression)
    throws SQLException, ExecutionException {
    org.apache.hadoop.hbase.client.Put conditionPut =
      getHBaseTableNameAndPut(expression.getName(), expression.getValue()).getSecond();

    byte[] dummyQualifier = getDummyQualifier();
    for (Map.Entry<byte[], List<Cell>> entry :
      conditionPut.getFamilyCellMap().entrySet()) {
      for (Cell cell : entry.getValue()) {
        if (!Bytes.equals(CellUtil.cloneQualifier(cell), dummyQualifier)) {
          return cell;
        }
      }
    }
    throw new ExecutionException("Can't get the condition cell");
  }

  private byte[] getDummyQualifier() {
    return tableMetaData.isColumnEncoded() ?
      DUMMY_COLUMN_QUALIFIER_IN_COLUMN_ENCODED : DUMMY_COLUMN_QUALIFIER;
  }

  private CompareOperator toCompareOperator(ConditionalExpression.Operator operator) {
    switch (operator) {
      case EQ:
        return CompareOperator.EQUAL;
      case NE:
        return CompareOperator.NOT_EQUAL;
      case GT:
        return CompareOperator.GREATER;
      case GTE:
        return CompareOperator.GREATER_OR_EQUAL;
      case LT:
        return CompareOperator.LESS;
      case LTE:
        return CompareOperator.LESS_OR_EQUAL;
      default:
        throw new AssertionError();
    }
  }

  public boolean getResult() {
    return result;
  }

  public boolean hasException() {
    return exception != null;
  }

  public ExecutionException getException() {
    return exception;
  }
}
