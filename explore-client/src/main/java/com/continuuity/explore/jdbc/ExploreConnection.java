package com.continuuity.explore.jdbc;

import com.continuuity.explore.client.ExternalAsyncExploreClient;
import com.continuuity.explore.service.Explore;

import java.net.URI;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

/**
 *
 */
public class ExploreConnection implements Connection {

  private final String host;
  private final int port;

  private final Explore exploreClient;

  public ExploreConnection(String uri, Properties info) {
    if (!uri.startsWith(ExploreJDBCUtils.URL_PREFIX)) {
      throw new IllegalArgumentException("Bad URL format: Missing prefix " + ExploreJDBCUtils.URL_PREFIX);
    }
    URI jdbcURI = URI.create(uri.substring(ExploreJDBCUtils.URI_JDBC_PREFIX.length()));
    host = jdbcURI.getHost();
    port = jdbcURI.getPort();

    exploreClient = new ExternalAsyncExploreClient(host, port);
  }

  @Override
  public Statement createStatement() throws SQLException {
    // TODO check if connection is closed and return exception in this case
    return new ExploreStatement(exploreClient);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    // TODO check if connection is closed and return exception in this case
    return new ExplorePreparedStatement(exploreClient, sql);
  }

  @Override
  public CallableStatement prepareCall(String s) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public String nativeSQL(String s) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setAutoCommit(boolean b) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void commit() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void rollback() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void close() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean isClosed() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setReadOnly(boolean b) throws SQLException {

  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setCatalog(String s) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public String getCatalog() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setTransactionIsolation(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Statement createStatement(int i, int i2) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i, int i2) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i2) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> stringClassMap) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setHoldability(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Savepoint setSavepoint(String s) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Statement createStatement(int i, int i2, int i3) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i, int i2, int i3) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i2, int i3) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean isValid(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setClientInfo(String s, String s2) throws SQLClientInfoException {

  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {

  }

  @Override
  public String getClientInfo(String s) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Array createArrayOf(String s, Object[] objects) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Struct createStruct(String s, Object[] objects) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    throw new SQLException("Method not supported");
  }
}