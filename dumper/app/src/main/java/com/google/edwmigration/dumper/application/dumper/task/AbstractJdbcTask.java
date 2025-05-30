/*
 * Copyright 2022-2025 Google LLC
 * Copyright 2013-2021 CompilerWorks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.edwmigration.dumper.application.dumper.task;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.ResultSetTransformer;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle.WriteMode;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Base64;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;

/** @author shevek */
public abstract class AbstractJdbcTask<T> extends AbstractTask<T> {

  private static final Logger logger = LoggerFactory.getLogger(AbstractJdbcTask.class);

  @CheckForNull private Class<? extends Enum<?>> headerClass;
  @CheckForNull private ResultSetTransformer<String[]> headerTransformer;

  public AbstractJdbcTask(@Nonnull String targetPath) {
    super(targetPath);
  }

  public AbstractJdbcTask(@Nonnull String targetPath, @Nonnull TaskOptions options) {
    super(targetPath, options);
  }

  @CheckForNull
  public Class<? extends Enum<?>> getHeaderClass() {
    return headerClass;
  }

  @Nonnull
  public AbstractJdbcTask<T> withHeaderClass(@Nonnull Class<? extends Enum<?>> headerClass) {
    this.headerClass = headerClass;
    return this;
  }

  @Nonnull
  public AbstractJdbcTask<T> withHeaderTransformer(
      @Nonnull ResultSetTransformer<String[]> headerTransformer) {
    this.headerTransformer = headerTransformer;
    return this;
  }

  @Nonnull
  protected CSVFormat newCsvFormat(@Nonnull ResultSet rs) throws SQLException {
    CSVFormat format = FORMAT;
    Class<? extends Enum<?>> headerClass = getHeaderClass();
    if (headerClass != null) {
      format = format.withHeader(headerClass);
      if (headerClass.getEnumConstants().length != rs.getMetaData().getColumnCount())
        // Can we avoid nesting exceptions here?
        throw new SQLException(
            new MetadataDumperUsageException(
                "Fatal Error. ResultSet does not have the expected column count: "
                    + headerClass.getEnumConstants().length,
                Arrays.asList(
                    "If a custom query has been specified please confirm the selected columns match"
                        + " the following: ",
                    StringUtils.join(headerClass.getEnumConstants(), ", "))));
    } else if (headerTransformer != null) {
      format = format.withHeader(headerTransformer.transform(rs));
    } else {
      format = format.withHeader(rs);
    }

    WriteMode writeMode = options.writeMode();
    switch (writeMode) {
      case CREATE_TRUNCATE:
        return format;
      case APPEND_EXISTING:
        return format.withSkipHeaderRecord(); // Header record already exists in case of append.
      default:
        throw new UnsupportedOperationException("Unsupported write mode: " + writeMode);
    }
  }

  @Nonnull
  public ResultSetExtractor<Summary> newCsvResultSetExtractor(@Nonnull ByteSink sink) {
    return rs -> {
      try (RecordProgressMonitor monitor = new RecordProgressMonitor(getName())) {
        printAllResults(sink, rs, monitor);
        return new Summary(monitor.getCount());
      } catch (IOException e) {
        throw new SQLException(e);
      }
    };
  }

  @Nonnull
  protected ResultSetExtractor<Summary> newCsvResultSetExtractor(
      @Nonnull ByteSink sink, long count) {
    return rs -> {
      try (RecordProgressMonitor monitor = new RecordProgressMonitor(getName(), count)) {
        printAllResults(sink, rs, monitor);
        return new Summary(monitor.getCount());
      } catch (IOException e) {
        throw new SQLException(e);
      }
    };
  }

  private void printAllResults(ByteSink sink, ResultSet resultSet, RecordProgressMonitor monitor)
      throws IOException, SQLException {
    CSVFormat format = newCsvFormat(resultSet);
    try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
        CSVPrinter printer = format.print(writer)) {
      int columnCount = resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        monitor.count();
        for (int i = 1; i <= columnCount; i++) {
          Object resultItem = resultSet.getObject(i);
          String csvItemCandidate = fromByteBufferOrClob(resultItem);
          String itemString;
          if (csvItemCandidate != null || resultItem == null) {
            // Item was recognized by the helper method or it was null.
            printer.print(csvItemCandidate);
          } else if ((itemString = resultItem.toString()) == null) {
            // Item violated usual toStringRules
            Class<?> itemClass = resultItem.getClass();
            logger.warn("Unexpected toString result for class {} - null", itemClass);
            printer.print(null);
          } else {
            printer.print(itemString);
          }
        }
        printer.println();
      }
    }
  }

  @Nullable
  private static String fromByteBufferOrClob(Object object) throws IOException, SQLException {
    if (object instanceof byte[]) {
      return Base64.getEncoder().encodeToString((byte[]) object);
    } else if (object instanceof Clob) {
      InputStream in = ((Clob) object).getAsciiStream();
      StringWriter w = new StringWriter();
      IOUtils.copy(in, w);
      return w.toString();
    } else {
      return null;
    }
  }

  public static ResultSetExtractor<Summary> withInterval(
      ResultSetExtractor<Summary> extractor, ZonedInterval interval) {
    return rs -> extractor.extractData(rs).withInterval(interval);
  }

  public static void setParameterValues(@Nonnull PreparedStatement statement, Object... arguments)
      throws SQLException {
    for (int i = 0; i < arguments.length; i++)
      StatementCreatorUtils.setParameterValue(
          statement, i + 1, SqlTypeValue.TYPE_UNKNOWN, arguments[i]);
  }

  @SuppressWarnings("UnusedMethod")
  private static void debug(@Nonnull Statement statement) throws SQLException {
    logger.debug(
        "Concurrency = "
            + statement.getResultSetConcurrency()
            + " (want "
            + ResultSet.CONCUR_READ_ONLY
            + ")");
    logger.debug(
        "Holdability = "
            + statement.getResultSetHoldability()
            + " (want "
            + ResultSet.CLOSE_CURSORS_AT_COMMIT
            + ")");
    logger.debug("FetchSize = " + statement.getFetchSize());
    logger.debug(
        "ResultSetType = "
            + statement.getResultSetType()
            + " (want "
            + ResultSet.TYPE_FORWARD_ONLY
            + ")");
    logger.debug("AutoCommit = " + statement.getConnection().getAutoCommit() + " (want false)");
  }

  // Very similar to JdbcTemplate, except works for bulk selects without blowing RAM.
  @CheckForNull
  protected static <T> T doSelect(
      @Nonnull Connection connection,
      @Nonnull ResultSetExtractor<T> resultSetExtractor,
      @Nonnull String sql,
      @Nonnull Object... arguments)
      throws SQLException {
    PreparedStatement statement = null;
    try {
      logger.debug("Preparing statement...");

      PREPARE:
      {
        Stopwatch stopwatch = Stopwatch.createStarted();
        // Causes PostgreSQL to use cursors, rather than RAM.
        // https://jdbc.postgresql.org/documentation/83/query.html#fetchsize-example
        // https://medium.com/@FranckPachot/oracle-postgres-jdbc-fetch-size-3012d494712
        connection.setAutoCommit(false);
        // connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);    // Shouldn't be
        // required.
        statement =
            connection.prepareStatement(
                sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        setParameterValues(statement, arguments);
        // statement.setFetchDirection(ResultSet.FETCH_FORWARD);   // PostgreSQL and (allegedly)
        // Teradata prefer this. However, it is the default, and sqlite throws.
        // Enables cursors in PostgreSQL.
        // Teradata says that this can reduce the fetch size below 1Mb, but not increase it.
        statement.setFetchSize(16384);
        logger.debug("Statement preparation took {}. Executing...", stopwatch);
      }

      EXECUTE:
      {
        // debug(statement);
        Stopwatch stopwatch = Stopwatch.createStarted();
        statement.execute(); // Must return true to indicate a ResultSet object.
        logger.debug("Statement execution took {}. Extracting results...", stopwatch);
        // debug(statement);
      }

      T result = null;
      ResultSet rs = null;
      try {
        Stopwatch stopwatch = Stopwatch.createStarted();
        rs = statement.getResultSet();
        result = resultSetExtractor.extractData(rs);
        logger.debug("Result set extraction took {}.", stopwatch);
      } finally {
        JdbcUtils.closeResultSet(rs);
      }

      SQLWarning warning = statement.getWarnings();
      while (warning != null) {
        logger.warn(
            "SQL warning: ["
                + warning.getSQLState()
                + "/"
                + warning.getErrorCode()
                + "] "
                + warning.getMessage());
        warning = warning.getNextWarning();
      }

      return result;
    } finally {
      JdbcUtils.closeStatement(statement);
    }
  }

  @CheckForNull
  protected abstract T doInConnection(
      @Nonnull TaskRunContext context,
      @Nonnull JdbcHandle jdbcHandle,
      @Nonnull ByteSink sink,
      @Nonnull Connection connection)
      throws SQLException;

  @Override
  protected T doRun(TaskRunContext context, ByteSink sink, Handle handle) throws Exception {
    JdbcHandle jdbcHandle = (JdbcHandle) handle;
    logger.info("Writing to {} -> {}", getTargetPath(), sink);

    DataSource dataSource = jdbcHandle.getDataSource();
    // We could use JdbcUtils, but that would prevent us from getting a .exception.txt.
    try (Connection connection = DataSourceUtils.getConnection(dataSource)) {
      // logger.debug("Connected to " + connection); // Hikari is using the same connection each
      // time.
      return doInConnection(context, jdbcHandle, sink, connection);
    }
  }
}
