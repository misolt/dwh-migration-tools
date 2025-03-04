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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static com.google.edwmigration.dumper.application.dumper.test.DumperTestUtils.assertQueryEquals;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.AbstractTeradataConnector.SharedState;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TeradataLogsJdbcTaskTest {

  private SharedState queryLogsState = new SharedState();

  @Test
  public void getOrCreateSql_success() {
    ZonedInterval interval =
        new ZonedInterval(
            ZonedDateTime.of(2023, 3, 4, 16, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2023, 3, 4, 17, 0, 0, 0, ZoneOffset.UTC));
    TeradataLogsJdbcTask jdbcTask =
        new TeradataLogsJdbcTask(
            "result.csv",
            queryLogsState,
            QueryLogTableNames.create(
                "SampleQueryTable", "SampleSqlTable", /* usingAtLeastOneAlternate */ true),
            /* conditions= */ ImmutableSet.of(),
            interval);

    // Act
    String query = jdbcTask.getOrCreateSql(s -> true, ImmutableList.of("L.QueryID", "ST.QueryID"));

    // Assert
    assertQueryEquals(
        "SELECT L.QueryID, ST.QueryID"
            + " FROM SampleQueryTable L LEFT OUTER JOIN SampleSqlTable ST ON (L.QueryID=ST.QueryID)"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP)",
        query);
  }

  @Test
  public void toString_success() {
    ZonedInterval interval =
        new ZonedInterval(
            ZonedDateTime.of(2023, 3, 4, 16, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2023, 3, 4, 17, 0, 0, 0, ZoneOffset.UTC));
    TeradataLogsJdbcTask jdbcTask =
        new TeradataLogsJdbcTask(
            "result.csv",
            queryLogsState,
            QueryLogTableNames.create(
                "SampleQueryTable", "SampleSqlTable", /* usingAtLeastOneAlternate */ true),
            /* conditions= */ ImmutableSet.of(),
            interval);
    String unused = jdbcTask.getOrCreateSql(s -> true, ImmutableList.of("L.QueryID", "ST.QueryID"));

    // Act
    String taskDescription = jdbcTask.toString();

    // Assert
    assertEquals(
        "Write result.csv from\n        SELECT L.QueryID, ST.QueryID"
            + " FROM SampleQueryTable L LEFT OUTER JOIN SampleSqlTable ST ON (L.QueryID=ST.QueryID)"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP)",
        taskDescription);
  }

  @Test
  public void getOrCreateSql_withCondition() {
    ZonedInterval interval =
        new ZonedInterval(
            ZonedDateTime.of(2023, 3, 4, 16, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2023, 3, 4, 17, 0, 0, 0, ZoneOffset.UTC));
    TeradataLogsJdbcTask jdbcTask =
        new TeradataLogsJdbcTask(
            "result.csv",
            queryLogsState,
            QueryLogTableNames.create(
                "SampleQueryTable", "SampleSqlTable", /* usingAtLeastOneAlternate */ true),
            /* conditions= */ ImmutableSet.of("L.UserName <> 'DBC'"),
            interval);

    // Act
    String query = jdbcTask.getOrCreateSql(s -> true, ImmutableList.of("L.QueryID", "ST.QueryID"));

    // Assert
    assertQueryEquals(
        "SELECT L.QueryID, ST.QueryID"
            + " FROM SampleQueryTable L LEFT OUTER JOIN SampleSqlTable ST ON (L.QueryID=ST.QueryID)"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP) AND L.UserName <> 'DBC'",
        query);
  }

  @Test
  public void toString_withZoneOffset_success() {
    ZonedInterval interval =
        new ZonedInterval(
            ZonedDateTime.of(2023, 3, 4, 16, 0, 0, 0, ZoneOffset.ofHours(3)),
            ZonedDateTime.of(2023, 3, 4, 17, 0, 0, 0, ZoneOffset.ofHours(3)));
    TeradataLogsJdbcTask jdbcTask =
        new TeradataLogsJdbcTask(
            "result.csv",
            queryLogsState,
            QueryLogTableNames.create(
                "SampleQueryTable", "SampleSqlTable", /* usingAtLeastOneAlternate */ true),
            /* conditions= */ ImmutableSet.of(),
            interval);
    String unused = jdbcTask.getOrCreateSql(s -> true, ImmutableList.of("L.QueryID", "ST.QueryID"));

    // Act
    String taskDescription = jdbcTask.toString();

    // Assert
    assertEquals(
        "Write result.csv from\n        SELECT L.QueryID, ST.QueryID"
            + " FROM SampleQueryTable L LEFT OUTER JOIN SampleSqlTable ST ON (L.QueryID=ST.QueryID)"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T13:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T14:00:00Z' AS TIMESTAMP)",
        taskDescription);
  }
}
