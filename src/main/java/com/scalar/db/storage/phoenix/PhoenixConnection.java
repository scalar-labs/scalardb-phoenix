package com.scalar.db.storage.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

public class PhoenixConnection {

  private final String jdbcUrl;
  private final Properties info;

  public PhoenixConnection(String jdbcUrl, Properties info) {
    this.jdbcUrl = Objects.requireNonNull(jdbcUrl);
    this.info = Objects.requireNonNull(info);
  }

  public Connection getConnection() throws SQLException {
    return getConnection(true);
  }

  public Connection getConnectionWithoutAutoCommit() throws SQLException {
    return getConnection(false);
  }

  private Connection getConnection(boolean autoCommit) throws SQLException {
    Connection connection;
    if (info != null) {
      connection = DriverManager.getConnection(jdbcUrl, info);
    } else {
      connection = DriverManager.getConnection(jdbcUrl);
    }

    connection.setAutoCommit(autoCommit);
    return connection;
  }
}
