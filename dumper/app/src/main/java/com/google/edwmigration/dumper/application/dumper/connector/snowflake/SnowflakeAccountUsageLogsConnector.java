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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeInput.USAGE_ONLY_SOURCE;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import javax.annotation.Nonnull;

/** @author shevek */
@AutoService(Connector.class)
public class SnowflakeAccountUsageLogsConnector extends SnowflakeLogsConnector {

  public SnowflakeAccountUsageLogsConnector() {
    super("snowflake-account-usage-logs", USAGE_ONLY_SOURCE);
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Dumps logs from Snowflake, using ACCOUNT_USAGE only.";
  }
}
