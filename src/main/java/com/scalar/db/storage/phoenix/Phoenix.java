package com.scalar.db.storage.phoenix;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;

public class Phoenix implements DistributedStorage {

  private final PhoenixConnection phoenixConnection;
  private final TableMetaDataManager tableMetaDataManager;
  private final OperationChecker operationChecker;
  private final Query query;
  private String defaultNamespace;
  private String defaultTableName;

  public Phoenix(String jdbcUrl, Properties info) {
    if (info == null) {
      info = new Properties();
    }
    info.put("phoenix.schema.isNamespaceMappingEnabled", "true");
    phoenixConnection = new PhoenixConnection(jdbcUrl, info);

    tableMetaDataManager = new TableMetaDataManager(phoenixConnection);
    operationChecker = new OperationChecker(tableMetaDataManager);
    query = new Query(tableMetaDataManager);
  }

  public Phoenix(String jdbcUrl) {
    this(jdbcUrl, null);
  }

  @Override
  public void with(String namespace, String tableName) {
    defaultNamespace = namespace;
    defaultTableName = tableName;
  }

  private TableName getTableName(Operation operation) {
    String schema = operation.forNamespace().orElse(defaultNamespace);
    String tableName = operation.forTable().orElse(defaultTableName);
    return new TableName(schema, tableName);
  }

  @Override
  public Optional<Result> get(Get get) throws ExecutionException {
    addProjectionsForKeys(get);
    TableName tableName = getTableName(get);
    @Nullable Key clusteringKey = get.getClusteringKey().orElse(null);

    operationChecker.checkGet(tableName, get.getProjections(), get.getPartitionKey(),
      clusteringKey);

    Query.SelectQuery selectQuery = query.select(get.getProjections()).from(tableName)
      .where(get.getPartitionKey(), clusteringKey).build();

    try (Connection connection = phoenixConnection.getConnection()) {
      PreparedStatement preparedStatement = selectQuery.prepare(connection);
      selectQuery.bind(preparedStatement);
      ResultSet resultSet = preparedStatement.executeQuery();
      if (resultSet.next()) {
        return Optional.of(selectQuery.getResult(resultSet));
      }
      return Optional.empty();
    } catch (SQLException e) {
      throw new ExecutionException("An error occurred in get()", e);
    }
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    addProjectionsForKeys(scan);
    TableName tableName = getTableName(scan);

    @Nullable Key startClusteringKey = scan.getStartClusteringKey().orElse(null);
    @Nullable Key endClusteringKey = scan.getEndClusteringKey().orElse(null);

    operationChecker.checkScan(tableName, scan.getProjections(), scan.getPartitionKey(),
      startClusteringKey, endClusteringKey, scan.getLimit(), scan.getOrderings());

    Query.SelectQuery selectQuery = query.select(scan.getProjections())
      .from(tableName)
      .where(scan.getPartitionKey(), startClusteringKey, scan.getStartInclusive(),
        endClusteringKey, scan.getEndInclusive())
      .orderBy(scan.getOrderings())
      .limit(scan.getLimit()).build();

    Connection connection = null;
    try {
      connection = phoenixConnection.getConnection();
      PreparedStatement preparedStatement = selectQuery.prepare(connection);
      selectQuery.bind(preparedStatement);
      ResultSet resultSet = preparedStatement.executeQuery();
      return new ScannerImpl(selectQuery, connection, resultSet);
    } catch (SQLException e) {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException sqlException) {
        throw new ExecutionException("An error occurred in scan()", sqlException);
      }

      throw new ExecutionException("An error occurred in scan()", e);
    }
  }

  private void addProjectionsForKeys(Selection selection) {
    if (selection.getProjections().size() == 0) { // meaning projecting all
      return;
    }
    selection.getPartitionKey().forEach(v -> selection.withProjection(v.getName()));
    selection.getClusteringKey()
      .ifPresent(k -> k.forEach(v -> selection.withProjection(v.getName())));
  }

  @Override
  public void put(Put put) throws ExecutionException {
    TableName tableName = getTableName(put);
    @Nullable Key clusteringKey = put.getClusteringKey().orElse(null);

    operationChecker.checkPut(tableName, put.getPartitionKey(), clusteringKey, put.getValues());

    if (put.getCondition().isPresent()) {
      ConditionalUpdater conditionalUpdater = new ConditionalUpdater(phoenixConnection, query,
        tableMetaDataManager.getTableMetaData(tableName), put.getPartitionKey(), clusteringKey,
        put);
      put.getCondition().get().accept(conditionalUpdater);

      if (conditionalUpdater.hasException()) {
        throw conditionalUpdater.getException();
      } else if (!conditionalUpdater.getResult()) {
        throw new NoMutationException("no mutation was applied");
      }
      return;
    }

    try (Connection connection = phoenixConnection.getConnection()) {
      createPreparedStatementForPut(connection, tableName, put.getPartitionKey(), clusteringKey,
        put.getValues()).executeUpdate();
    } catch (SQLException e) {
      throw new ExecutionException("An error occurred in put()", e);
    }
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    TableName tableName = getTableName(delete);
    @Nullable Key clusteringKey = delete.getClusteringKey().orElse(null);

    operationChecker.checkDelete(tableName, delete.getPartitionKey(), clusteringKey);

    if (delete.getCondition().isPresent()) {
      ConditionalUpdater conditionalUpdater = new ConditionalUpdater(phoenixConnection, query,
        tableMetaDataManager.getTableMetaData(tableName), delete.getPartitionKey(), clusteringKey,
        delete);
      delete.getCondition().get().accept(conditionalUpdater);

      if (conditionalUpdater.hasException()) {
        throw conditionalUpdater.getException();
      } else if (!conditionalUpdater.getResult()) {
        throw new NoMutationException("no mutation was applied");
      }
      return;
    }

    try (Connection connection = phoenixConnection.getConnection()) {
      createPreparedStatementForDelete(connection, tableName, delete.getPartitionKey(),
        clusteringKey).executeUpdate();
    } catch (SQLException e) {
      throw new ExecutionException("An error occurred in delete()", e);
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws ExecutionException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {
    checkArgument(!mutations.isEmpty());

    if (mutations.size() == 1) {
      Mutation mutation = mutations.get(0);
      if (mutation instanceof Put) {
        put((Put) mutation);
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
      }
      return;
    }

    operationChecker.checkMutations(mutations);

    try (Connection connection = phoenixConnection.getConnectionWithoutAutoCommit()) {
      for (Mutation mutation : mutations) {
        TableName tableName = getTableName(mutation);
        @Nullable Key clusteringKey = mutation.getClusteringKey().orElse(null);

        if (mutation.getCondition().isPresent()) {
          // TODO Make this multi-threaded
          ConditionalUpdater conditionalUpdater = new ConditionalUpdater(phoenixConnection, query,
            tableMetaDataManager.getTableMetaData(tableName), mutation.getPartitionKey(),
            clusteringKey, mutation);
          mutation.getCondition().get().accept(conditionalUpdater);
          if (conditionalUpdater.hasException()) {
            throw conditionalUpdater.getException();
          } else if (!conditionalUpdater.getResult()) {
            throw new NoMutationException("no mutation was applied");
          }
          continue;
        }

        if (mutation instanceof Put) {
          Put put = (Put) mutation;
          operationChecker.checkPut(tableName, put.getPartitionKey(), clusteringKey,
            put.getValues());
          createPreparedStatementForPut(connection, tableName, put.getPartitionKey(),
            clusteringKey, put.getValues()).executeUpdate();
        } else if (mutation instanceof Delete) {
          Delete delete = (Delete) mutation;
          operationChecker.checkDelete(tableName, delete.getPartitionKey(), clusteringKey);
          createPreparedStatementForDelete(connection, tableName, delete.getPartitionKey(),
            clusteringKey).executeUpdate();
        }
      }
      connection.commit();
    } catch (SQLException e) {
      throw new ExecutionException("An error occurred in mutate()", e);
    }
  }

  private PreparedStatement createPreparedStatementForPut(Connection connection,
    TableName tableName, Key partitionKey, @Nullable Key clusteringKey,
    Map<String, Value> values) throws ExecutionException {

    Query.UpsertQuery upsertQuery = query.upsert().into(tableName)
      .values(partitionKey, clusteringKey, values).build();

    PreparedStatement preparedStatement = upsertQuery.prepare(connection);
    upsertQuery.bind(preparedStatement);
    return preparedStatement;
  }

  private PreparedStatement createPreparedStatementForDelete(Connection connection,
    TableName tableName, Key partitionKey, @Nullable Key clusteringKey)
    throws ExecutionException {

    Query.DeleteQuery deleteQuery = query.delete().from(tableName)
      .where(partitionKey, clusteringKey).build();

    PreparedStatement preparedStatement = deleteQuery.prepare(connection);
    deleteQuery.bind(preparedStatement);
    return preparedStatement;
  }

  @Override
  public void close() {
    // Do nothing
  }

  public PhoenixConnection getPhoenixConnection() {
    return phoenixConnection;
  }

  public void clearTableMetaDataCache() {
    tableMetaDataManager.clearTableMetaDataCache();
  }
}
