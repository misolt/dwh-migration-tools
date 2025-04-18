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
package com.google.edwmigration.dumper.application.dumper;

import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_HOST_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConnectorArgumentsTest {

  @Test
  public void isPasswordProvided_success() throws Exception {
    {
      ConnectorArguments arguments = new ConnectorArguments("--connector", "simple", "--password");
      assertTrue(arguments.isPasswordFlagProvided());
    }
    {
      ConnectorArguments arguments =
          new ConnectorArguments("--connector", "simple", "--password", "secret");
      assertTrue(arguments.isPasswordFlagProvided());
    }
  }

  @Test
  public void isPasswordProvided_false() throws Exception {
    ConnectorArguments arguments = new ConnectorArguments("--connector", "simple");
    assertFalse(arguments.isPasswordFlagProvided());
  }

  @Test
  public void getDatabases_success() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments("--connector", "teradata", "--database", "sample-db");

    List<String> databaseNames = arguments.getDatabases();

    assertEquals(ImmutableList.of("sample-db"), databaseNames);
  }

  @Test
  public void getDatabases_databaseOptionNotSpecified_success() throws IOException {
    ConnectorArguments arguments = new ConnectorArguments("--connector", "teradata");

    List<String> databaseNames = arguments.getDatabases();

    assertTrue(databaseNames.isEmpty());
  }

  @Test
  public void getDatabases_trimDatabaseNames() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments("--connector", "teradata", "--database", "db1, db2 ");

    List<String> databaseNames = arguments.getDatabases();

    assertEquals(ImmutableList.of("db1", "db2"), databaseNames);
  }

  @Test
  public void getDatabases_trimDatabaseNamesFilteringOutBlankStrings() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments("--connector", "teradata", "--database", "db1, ,,, db2 ");

    List<String> databaseNames = arguments.getDatabases();

    assertEquals(ImmutableList.of("db1", "db2"), databaseNames);
  }

  @Test
  public void getHost_notProvidedInFlag_useArgument() {
    ConnectorArguments arguments = arguments("--connector", "oracle");

    assertEquals("default-host-789", arguments.getHost("default-host-789"));
  }

  @Test
  public void getHost_providedInFlag_useFlagAndIgnoreArgument() {
    ConnectorArguments arguments = arguments("--connector", "oracle", "--host", "example-host-123");

    assertEquals("example-host-123", arguments.getHost("default-host-789"));
  }

  @Test
  public void getHostOrDefault_notProvidedInFlag_useDefault() {
    ConnectorArguments arguments = arguments("--connector", "oracle");

    assertEquals(OPT_HOST_DEFAULT, arguments.getHostOrDefault());
  }

  @Test
  public void getHostOrDefault_providedInFlag_useFlag() {
    ConnectorArguments arguments = arguments("--connector", "oracle", "--host", "example-host-123");

    assertEquals("example-host-123", arguments.getHostOrDefault());
  }

  @Test
  public void getHost_notProvidedInFlag_returnNull() {
    ConnectorArguments arguments = arguments("--connector", "oracle");

    assertNull(arguments.getHost());
  }

  @Test
  public void getHost_providedInFlag_useFlag() {
    ConnectorArguments arguments = arguments("--connector", "oracle", "--host", "example-host-123");

    assertEquals("example-host-123", arguments.getHost());
  }

  @Test
  public void getUserOrFail_noUserFlag_throwsException() throws IOException {
    ConnectorArguments arguments = new ConnectorArguments("--connector", "abcABC123");

    Exception exception =
        assertThrows(MetadataDumperUsageException.class, arguments::getUserOrFail);

    assertEquals(
        "Required username was not provided. Please use the '--user' flag to provide the username.",
        exception.getMessage());
  }

  @Test
  public void getUserOrFail_success() throws IOException {
    String expectedName = "admin456";
    ConnectorArguments arguments =
        new ConnectorArguments("--connector", "abcABC123", "--user", expectedName);

    String actualName = arguments.getUserOrFail();

    assertEquals(expectedName, actualName);
  }

  // helper method to suppress IOException
  private static ConnectorArguments arguments(String... terms) {
    try {
      return new ConnectorArguments(terms);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
