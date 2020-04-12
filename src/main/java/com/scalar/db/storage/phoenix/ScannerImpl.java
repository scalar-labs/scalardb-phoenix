package com.scalar.db.storage.phoenix;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ScannerImpl implements Scanner {

  private final Query.SelectQuery selectQuery;
  private final Connection connection;
  private final ResultSet resultSet;

  public ScannerImpl(Query.SelectQuery selectQuery, Connection connection, ResultSet resultSet) {
    this.selectQuery = Objects.requireNonNull(selectQuery);
    this.connection = Objects.requireNonNull(connection);
    this.resultSet = Objects.requireNonNull(resultSet);
  }

  @Override
  public Optional<Result> one() throws ExecutionException {
    try {
      if (resultSet.next()) {
        return Optional.of(selectQuery.getResult(resultSet));
      }
      return Optional.empty();
    } catch (SQLException e) {
      throw new ExecutionException("An error occurred in one()", e);
    }
  }

  @Override
  public List<Result> all() throws ExecutionException {
    List<Result> ret = new ArrayList<>();
    while (true) {
      Optional<Result> one = one();
      if (one.isPresent()) {
        ret.add(one.get());
      } else {
        break;
      }
    }
    return ret;
  }

  @Override
  @Nonnull
  public Iterator<Result> iterator() {
    return new ScannerIterator(this);
  }

  @Override
  public void close() throws IOException {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new IOException("An error occurred in close()", e);
    }
  }
}
