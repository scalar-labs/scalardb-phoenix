package com.scalar.db.storage.phoenix;

import com.scalar.db.api.Mutation;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.InvalidUsageException;
import com.scalar.db.exception.storage.MultiPartitionException;
import com.scalar.db.exception.storage.RetriableExecutionException;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OperationChecker {

  private static class TypeChecker implements ValueVisitor {
    private final TableMetaData tableMetaData;
    private boolean okay;

    public TypeChecker(TableMetaData tableMetaData) {
      this.tableMetaData = tableMetaData;
    }

    private boolean check(Value value) {
      value.accept(this);
      return okay;
    }

    @Override
    public void visit(BooleanValue value) {
      okay = tableMetaData.getColumnType(value.getName()) == Type.BOOLEAN;
    }

    @Override
    public void visit(IntValue value) {
      okay = tableMetaData.getColumnType(value.getName()) == Type.INT;
    }

    @Override
    public void visit(BigIntValue value) {
      okay = tableMetaData.getColumnType(value.getName()) == Type.BIGINT;
    }

    @Override
    public void visit(FloatValue value) {
      okay = tableMetaData.getColumnType(value.getName()) == Type.FLOAT;
    }

    @Override
    public void visit(DoubleValue value) {
      okay = tableMetaData.getColumnType(value.getName()) == Type.DOUBLE;
    }

    @Override
    public void visit(TextValue value) {
      okay = tableMetaData.getColumnType(value.getName()) == Type.TEXT;
    }

    @Override
    public void visit(BlobValue value) {
      okay = tableMetaData.getColumnType(value.getName()) == Type.BLOB;
    }
  }

  private final TableMetaDataManager tableMetaDataManager;

  public OperationChecker(TableMetaDataManager tableMetaDataManager) {
    this.tableMetaDataManager = Objects.requireNonNull(tableMetaDataManager);
  }

  public void checkGet(TableName tableName, List<String> projections, Key partitionKey,
    @Nullable Key clusteringKey) throws ExecutionException {

    TableMetaData tableMetaData = tableMetaDataManager.getTableMetaData(tableName);

    checkProjections(tableMetaData, projections);
    checkKeys(tableMetaData, partitionKey, clusteringKey, false);
  }

  public void checkScan(TableName tableName, List<String> projections, Key partitionKey,
    @Nullable Key startClusteringKey, @Nullable Key endClusteringKey, int limit,
    List<Scan.Ordering> orderings) throws ExecutionException {
    TableMetaData tableMetaData = tableMetaDataManager.getTableMetaData(tableName);

    checkProjections(tableMetaData, projections);

    if (startClusteringKey != null) {
      checkKeys(tableMetaData, partitionKey, startClusteringKey, true);
    }

    if (endClusteringKey != null) {
      checkKeys(tableMetaData, partitionKey, endClusteringKey, true);
    }

    if (startClusteringKey == null && endClusteringKey == null) {
      checkKeys(tableMetaData, partitionKey, null, true);
    }

    if (startClusteringKey != null && endClusteringKey != null) {
      checkClusteringKeyRange(startClusteringKey, endClusteringKey);
    }

    if (limit < 0) {
      throw new InvalidUsageException("limit must not negative");
    }

    // TODO check orderings
  }

  public void checkPut(TableName tableName, Key partitionKey, @Nullable Key clusteringKey,
    Map<String, Value> values) throws ExecutionException {
    TableMetaData tableMetaData = tableMetaDataManager.getTableMetaData(tableName);
    checkKeys(tableMetaData, partitionKey, clusteringKey, false);
    checkValues(tableMetaData, values);
  }

  public void checkDelete(TableName tableName, Key partitionKey, @Nullable Key clusteringKey)
    throws ExecutionException {
    TableMetaData tableMetaData = tableMetaDataManager.getTableMetaData(tableName);
    checkKeys(tableMetaData, partitionKey, clusteringKey, true);
  }

  private void checkProjections(TableMetaData tableMetaData, List<String> projections) {
    for (String projection : projections) {
      if (!tableMetaData.columnExists(projection)) {
        throw new InvalidUsageException("the projection is not found in the table metadata: "
          + projection);
      }
    }
  }

  private void checkKeys(TableMetaData tableMetaData, Key partitionKey,
    @Nullable Key clusteringKey, boolean allowPartial) {

    List<Value> values = new ArrayList<>(partitionKey.get());
    if (clusteringKey != null) {
      values.addAll(clusteringKey.get());
    }

    List<String> primaryKeys = tableMetaData.getPrimaryKeys();

    if (!allowPartial) {
      if (values.size() != primaryKeys.size()) {
        throw new InvalidUsageException("The specified keys are invalid.");
      }
    } else {
      if (values.size() > primaryKeys.size()) {
        throw new InvalidUsageException("The specified keys are invalid.");
      }
    }

    for (int i = 0; i < values.size(); i++) {
      String primaryKey = primaryKeys.get(i);
      Value value = values.get(i);

      if (!primaryKey.equals(value.getName())) {
        throw new InvalidUsageException("The specified keys are invalid.");
      }

      if (!new TypeChecker(tableMetaData).check(value)) {
        throw new InvalidUsageException("The specified keys are invalid.");
      }
    }
  }

  private void checkClusteringKeyRange(Key startClusteringKey, Key endClusteringKey) {
    if (startClusteringKey.size() != endClusteringKey.size()) {
      throw new InvalidUsageException("The specified clustering keys are invalid.");
    }

    for (int i = 0; i < startClusteringKey.size() - 1; i++) {
      Value startValue = startClusteringKey.get().get(i);
      Value endValue = endClusteringKey.get().get(i);
      if (!startValue.equals(endValue)) {
        throw new InvalidUsageException("The specified clustering keys are invalid.");
      }
    }
  }

  private void checkValues(TableMetaData tableMetaData, Map<String, Value> values) {
    for (Map.Entry<String, Value> entry : values.entrySet()) {
      if (!new TypeChecker(tableMetaData).check(entry.getValue())) {
        throw new InvalidUsageException("The specified values are invalid.");
      }
    }
  }

  public void checkMutations(List<? extends Mutation> mutations) throws RetriableExecutionException {
    Mutation first = mutations.get(0);
    for (Mutation mutation : mutations) {
      if (!mutation.forTable().equals(first.forTable())
        || !mutation.getPartitionKey().equals(first.getPartitionKey())) {

        RuntimeException e = new MultiPartitionException(
          "decided not to execute this batch since multi-partition batch is not recommended.");
        throw new RetriableExecutionException(e.getMessage(), e);
      }
    }
  }
}
