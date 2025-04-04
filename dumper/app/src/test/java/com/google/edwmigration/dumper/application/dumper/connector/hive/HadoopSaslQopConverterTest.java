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
package com.google.edwmigration.dumper.application.dumper.connector.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HadoopSaslQopConverterTest {

  @Test
  public void convert_success() {
    String qop = HadoopSaslQopConverter.convert("integrity").get();

    assertEquals("auth-int", qop);
  }

  @Test
  public void convert_emptyString() {
    assertFalse(HadoopSaslQopConverter.convert("").isPresent());
  }

  @Test
  public void convert_invalidString_fail() {
    MetadataDumperUsageException exception =
        Assert.assertThrows(
            MetadataDumperUsageException.class,
            () -> {
              Optional<String> ignored = HadoopSaslQopConverter.convert("auth");
            });

    assertEquals(
        "Invalid value 'auth' for property 'hiveql.rpc.protection'."
            + " Allowed values are: 'authentication, integrity, privacy'.",
        exception.getMessage());
  }
}
