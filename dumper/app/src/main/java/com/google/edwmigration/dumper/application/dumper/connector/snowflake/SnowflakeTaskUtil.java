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

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import java.util.Collection;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
final class SnowflakeTaskUtil {
  private static final String EMPTY_WHERE_CLAUSE = "";

  static AbstractJdbcTask<Summary> withFilter(
      String format,
      String schemaName,
      String zipEntryName,
      Collection<String> whereConditions,
      Class<? extends Enum<?>> header) {
    String sql = String.format(format, schemaName, getWhereClause(whereConditions));
    return new JdbcSelectTask(zipEntryName, sql).withHeaderClass(header);
  }

  private static String getWhereClause(Collection<String> whereConditions) {
    ImmutableList<String> conditions =
        whereConditions.stream().filter(c -> !Strings.isNullOrEmpty(c)).collect(toImmutableList());
    if (conditions.isEmpty()) {
      return EMPTY_WHERE_CLAUSE;
    }
    return " WHERE " + Joiner.on(" AND ").join(conditions);
  }

  private SnowflakeTaskUtil() {}
}
