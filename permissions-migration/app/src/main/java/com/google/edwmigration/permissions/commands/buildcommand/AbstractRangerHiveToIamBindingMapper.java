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
package com.google.edwmigration.permissions.commands.buildcommand;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.models.Table;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy.PolicyItemAccess;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy.PolicyResource;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRangerHiveToIamBindingMapper extends AbstractRangerPermissionMapper {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractRangerHiveToIamBindingMapper.class);

  private static final String RANGER_HIVE_SERVICE = "hive";

  private static final String RANGER_DATABASE_RESOURCE = "database";

  private static final String RANGER_TABLE_RESOURCE = "table";

  private static final ImmutableSet<String> RANGER_WRITE_ACCESSES =
      ImmutableSet.of(
          "update", "create", "drop", "alter", "index", "lock", "all", "write", "refresh");

  private static final ImmutableSet<String> RANGER_READ_ACCESSES =
      ImmutableSet.of("select", "read");

  private final String readIamRole;

  private final String writeIamRole;

  AbstractRangerHiveToIamBindingMapper(
      ImmutableList<Rule> rules,
      TableReader tableReader,
      PrincipalReader principalReader,
      RangerPolicyReader rangerPolicyReader,
      RangerServiceReader rangerServiceReader,
      String readIamRole,
      String writeIamRole) {
    super(
        "Hive",
        rules,
        tableReader,
        principalReader,
        rangerPolicyReader,
        rangerServiceReader,
        RANGER_HIVE_SERVICE);
    this.readIamRole = readIamRole;
    this.writeIamRole = writeIamRole;
  }

  @Override
  protected boolean policyMatchesTable(Policy policy, Table table) {
    Map<String, PolicyResource> resources = policy.resources();
    // TODO(aleofreddi): filter for Hive services.
    // TODO(aleofreddi): it's unclear to me if Ranger supports just '*' as a placeholder
    // for any value or it has a complete support for wildcard expansion (like `*sales`,
    //  `sales*`, or even `*some*thing*`). If that's the case, we should take care of
    // these here.
    if (resources.getOrDefault(RANGER_DATABASE_RESOURCE, ANY_RESOURCE).values().stream()
        .noneMatch(database -> database.equals("*") || database.equals(table.schemaName()))) {
      LOG.debug(
          "Table '{}'.'{}' does not match policy '{}' database resource",
          table.schemaName(),
          table.fullName(),
          policy.name());
      return false;
    }
    if (resources.getOrDefault(RANGER_TABLE_RESOURCE, ANY_RESOURCE).values().stream()
        .noneMatch(tableName -> tableName.equals("*") || tableName.equals(table.name()))) {
      LOG.debug(
          "Table '{}' does not match policy '{}' table resource", table.fullName(), policy.name());
      return false;
    }
    return true;
  }

  @Override
  protected Optional<String> getRoleForAccesses(List<PolicyItemAccess> accesses) {
    ImmutableSet<String> accessesSet =
        accesses.stream()
            .filter(PolicyItemAccess::isAllowed)
            .map(PolicyItemAccess::type)
            .collect(toImmutableSet());
    if (!intersection(accessesSet, RANGER_WRITE_ACCESSES).isEmpty()) {
      return Optional.of(writeIamRole);
    }
    if (!intersection(accessesSet, RANGER_READ_ACCESSES).isEmpty()) {
      return Optional.of(readIamRole);
    }
    return Optional.empty();
  }
}
