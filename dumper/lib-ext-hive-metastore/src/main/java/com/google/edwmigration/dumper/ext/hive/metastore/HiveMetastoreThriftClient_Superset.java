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
package com.google.edwmigration.dumper.ext.hive.metastore;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.edwmigration.dumper.ext.hive.metastore.MetastoreConstants.DDL_TIME;
import static com.google.edwmigration.dumper.ext.hive.metastore.MetastoreConstants.FILES_COUNT;
import static com.google.edwmigration.dumper.ext.hive.metastore.MetastoreConstants.RAW_SIZE;
import static com.google.edwmigration.dumper.ext.hive.metastore.MetastoreConstants.ROWS_COUNT;
import static com.google.edwmigration.dumper.ext.hive.metastore.MetastoreConstants.TOTAL_SIZE;
import static com.google.edwmigration.dumper.ext.hive.metastore.utils.PartitionNameGenerator.makePartitionName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.CheckConstraintsRequest;
import com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.DefaultConstraintsRequest;
import com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.FieldSchema;
import com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.ForeignKeysRequest;
import com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.GetCatalogRequest;
import com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.GetCatalogsResponse;
import com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.NotNullConstraintsRequest;
import com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.PrimaryKeysRequest;
import com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.TableStatsRequest;
import com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.UniqueConstraintsRequest;
import com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.WMGetAllResourcePlanRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses the Thrift specification known to us to be a superset of the Thrift specifications used in
 * other Hive versions.
 *
 * <p>This class is not thread-safe because it wraps an underlying Thrift client which itself is not
 * thread-safe.
 */
@NotThreadSafe
public class HiveMetastoreThriftClient_Superset extends HiveMetastoreThriftClient {

  @SuppressWarnings("UnusedVariable")
  private static final Logger logger =
      LoggerFactory.getLogger(HiveMetastoreThriftClient_Superset.class);

  @Nonnull
  private final com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset
          .ThriftHiveMetastore.Client
      client;

  // Deliberately not public
  /* pp */ HiveMetastoreThriftClient_Superset(@Nonnull String name, @Nonnull TProtocol protocol) {
    super(name);
    this.client =
        new com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset
            .ThriftHiveMetastore.Client(protocol);
  }

  @Nonnull
  @Override
  public ImmutableList<String> getAllDatabaseNames() throws Exception {
    return ImmutableList.copyOf(client.get_all_databases());
  }

  @Nonnull
  @Override
  public Database getDatabase(String databaseName) throws Exception {
    com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.Database database =
        client.get_database(databaseName);
    return new Database() {
      @CheckForNull
      @Override
      public String getName() {
        return database.getName();
      }

      @CheckForNull
      @Override
      public String getDescription() {
        return database.getDescription();
      }

      @CheckForNull
      @Override
      public String getOwner() {
        return database.getOwnerName();
      }

      @CheckForNull
      @Override
      public Integer getOwnerType() {
        return database.getOwnerType().getValue();
      }

      @CheckForNull
      @Override
      public String getLocation() {
        return database.getLocationUri();
      }
    };
  }

  @Override
  public ImmutableList<? extends TBase<?, ?>> getRawDatabases() throws Exception {
    return client.get_all_databases().stream()
        .map(
            databaseName -> {
              try {
                return client.get_database(databaseName);
              } catch (TException e) {
                throw new IllegalStateException(e);
              }
            })
        .collect(toImmutableList());
  }

  @Override
  public ImmutableList<String> getMasterKeys() throws Exception {
    return ImmutableList.copyOf(client.get_master_keys());
  }

  @Override
  public ImmutableList<DelegationToken> getDelegationTokens() throws Exception {
    return client.get_all_token_identifiers().stream()
        .map(
            tokenIdentifier -> {
              try {
                return DelegationToken.create(tokenIdentifier, client.get_token(tokenIdentifier));
              } catch (TException e) {
                throw new IllegalStateException(e);
              }
            })
        .collect(toImmutableList());
  }

  @Nonnull
  @Override
  public ImmutableList<String> getAllTableNamesInDatabase(@Nonnull String databaseName)
      throws Exception {
    return ImmutableList.copyOf(client.get_all_tables(databaseName));
  }

  @Nonnull
  @Override
  public Table getTable(@Nonnull String databaseName, @Nonnull String tableName) throws Exception {
    com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.Table table =
        client.get_table(databaseName, tableName);
    Map<String, String> parameters =
        table.isSetParameters() ? table.getParameters() : new HashMap<>();

    return new Table() {
      @CheckForNull
      @Override
      public String getDatabaseName() {
        return (table.isSetDbName() ? table.getDbName() : null);
      }

      @CheckForNull
      @Override
      public String getTableName() {
        return (table.isSetTableName() ? table.getTableName() : null);
      }

      @CheckForNull
      @Override
      public String getTableType() {
        return (table.isSetTableType() ? table.getTableType() : null);
      }

      @CheckForNull
      @Override
      public Integer getCreateTime() {
        return (table.isSetCreateTime() ? table.getCreateTime() : null);
      }

      @CheckForNull
      @Override
      public Integer getLastAccessTime() {
        return (table.isSetLastAccessTime() ? table.getLastAccessTime() : null);
      }

      @CheckForNull
      @Override
      public String getOwner() {
        return (table.isSetOwner() ? table.getOwner() : null);
      }

      @CheckForNull
      @Override
      public String getOriginalViewText() {
        return (table.isSetViewOriginalText() ? table.getViewOriginalText() : null);
      }

      @CheckForNull
      @Override
      public String getExpandedViewText() {
        return (table.isSetViewExpandedText() ? table.getViewExpandedText() : null);
      }

      @CheckForNull
      @Override
      public String getLocation() {
        return (table.isSetSd() && table.getSd().isSetLocation()
            ? table.getSd().getLocation()
            : null);
      }

      @CheckForNull
      @Override
      public Integer getLastDdlTime() {
        return parameters.containsKey(DDL_TIME) ? Integer.parseInt(parameters.get(DDL_TIME)) : null;
      }

      @CheckForNull
      @Override
      public Long getTotalSize() {
        return parameters.containsKey(TOTAL_SIZE)
            ? Long.parseLong(parameters.get(TOTAL_SIZE))
            : null;
      }

      @CheckForNull
      @Override
      public Long getRawSize() {
        return parameters.containsKey(RAW_SIZE) ? Long.parseLong(parameters.get(RAW_SIZE)) : null;
      }

      @CheckForNull
      @Override
      public Long getRowsCount() {
        return parameters.containsKey(ROWS_COUNT)
            ? Long.parseLong(parameters.get(ROWS_COUNT))
            : null;
      }

      @CheckForNull
      @Override
      public Integer getFilesCount() {
        return parameters.containsKey(FILES_COUNT)
            ? Integer.parseInt(parameters.get(FILES_COUNT))
            : null;
      }

      @CheckForNull
      @Override
      public Integer getRetention() {
        return table.getRetention();
      }

      @CheckForNull
      @Override
      public Integer getBucketsCount() {
        return table.isSetSd() ? table.getSd().getNumBuckets() : null;
      }

      @CheckForNull
      @Override
      public Boolean isCompressed() {
        return table.isSetSd() && table.getSd().isCompressed();
      }

      @CheckForNull
      @Override
      public String getSerializationLib() {
        return (table.isSetSd()
                && table.getSd().isSetSerdeInfo()
                && table.getSd().getSerdeInfo().isSetSerializationLib()
            ? table.getSd().getSerdeInfo().getSerializationLib()
            : null);
      }

      @CheckForNull
      @Override
      public String getInputFormat() {
        return (table.isSetSd() && table.getSd().isSetInputFormat()
            ? table.getSd().getInputFormat()
            : null);
      }

      @CheckForNull
      @Override
      public String getOutputFormat() {
        return (table.isSetSd() && table.getSd().isSetOutputFormat()
            ? table.getSd().getOutputFormat()
            : null);
      }

      @Nonnull
      @Override
      public List<? extends Field> getFields() throws Exception {
        // If we already have a non-null Storage Descriptor let's get the columns from it, we do one
        // less remote call and we also avoid
        // an exception "Storage schema reading not supported" due to OpenCSVSerde based tables. If
        // this is null we fall-back to calling get_fields.
        if (table.getSd() != null) {
          List<com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.FieldSchema>
              cols = table.getSd().getCols();
          if (cols != null) {
            return cols.stream().map(this::toField).collect(Collectors.toList());
          }
        }
        List<Field> out = new ArrayList<>();
        for (com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.FieldSchema
            field : client.get_fields(databaseName, tableName)) {
          out.add(toField(field));
        }
        return out;
      }

      @Nonnull
      private Field toField(
          com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.FieldSchema field) {
        return new Field() {
          @CheckForNull
          @Override
          public String getFieldName() {
            return (field.isSetName() ? field.getName() : null);
          }

          @CheckForNull
          @Override
          public String getType() {
            return (field.isSetType() ? field.getType() : null);
          }

          @CheckForNull
          @Override
          public String getComment() {
            return (field.isSetComment() ? field.getComment() : null);
          }
        };
      }

      @Nonnull
      @Override
      public List<? extends PartitionKey> getPartitionKeys() {
        List<PartitionKey> out = new ArrayList<>();
        for (com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.FieldSchema
            partitionKey : table.getPartitionKeys()) {
          out.add(
              new PartitionKey() {
                @CheckForNull
                @Override
                public String getPartitionKeyName() {
                  return (partitionKey.isSetName() ? partitionKey.getName() : null);
                }

                @CheckForNull
                @Override
                public String getType() {
                  return (partitionKey.isSetType() ? partitionKey.getType() : null);
                }

                @CheckForNull
                @Override
                public String getComment() {
                  return (partitionKey.isSetComment() ? partitionKey.getComment() : null);
                }
              });
        }
        return out;
      }

      @Nonnull
      @Override
      public List<? extends Partition> getPartitions() throws Exception {
        ImmutableList<String> partitionKeys =
            table.getPartitionKeys().stream().map(FieldSchema::getName).collect(toImmutableList());
        List<com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.Partition>
            partitionsMetadata = client.get_partitions(databaseName, tableName, (short) -1);

        return partitionsMetadata.stream()
            .map(
                partition -> {
                  String partitionName = makePartitionName(partitionKeys, partition.getValues());
                  Map<String, String> partitionParameters =
                      partition.isSetParameters() ? partition.getParameters() : ImmutableMap.of();

                  return new Partition() {
                    @Nonnull
                    @Override
                    public String getPartitionName() {
                      return partitionName;
                    }

                    @CheckForNull
                    @Override
                    public String getLocation() {
                      return (partition.isSetSd() && partition.getSd().isSetLocation()
                          ? partition.getSd().getLocation()
                          : null);
                    }

                    @CheckForNull
                    @Override
                    public Integer getCreateTime() {
                      return partition.getCreateTime();
                    }

                    @CheckForNull
                    @Override
                    public Integer getLastAccessTime() {
                      return partition.getLastAccessTime();
                    }

                    @CheckForNull
                    @Override
                    public Integer getLastDdlTime() {
                      return partitionParameters.containsKey(DDL_TIME)
                          ? Integer.parseInt(partitionParameters.get(DDL_TIME))
                          : null;
                    }

                    @CheckForNull
                    @Override
                    public Long getTotalSize() {
                      return partitionParameters.containsKey(TOTAL_SIZE)
                          ? Long.parseLong(partitionParameters.get(TOTAL_SIZE))
                          : null;
                    }

                    @CheckForNull
                    @Override
                    public Long getRawSize() {
                      return partitionParameters.containsKey(RAW_SIZE)
                          ? Long.parseLong(partitionParameters.get(RAW_SIZE))
                          : null;
                    }

                    @CheckForNull
                    @Override
                    public Long getRowsCount() {
                      return partitionParameters.containsKey(ROWS_COUNT)
                          ? Long.parseLong(partitionParameters.get(ROWS_COUNT))
                          : null;
                    }

                    @CheckForNull
                    @Override
                    public Integer getFilesCount() {
                      return partitionParameters.containsKey(FILES_COUNT)
                          ? Integer.parseInt(partitionParameters.get(FILES_COUNT))
                          : null;
                    }

                    @CheckForNull
                    @Override
                    public Boolean isCompressed() {
                      return partition.isSetSd() && partition.getSd().isCompressed();
                    }
                  };
                })
            .collect(toImmutableList());
      }

      @Override
      public TBase<?, ?> getRawThriftObject() {
        return table;
      }

      @Override
      public ImmutableList<? extends TBase<?, ?>> getRawPrimaryKeys() throws Exception {
        return ImmutableList.copyOf(
            client
                .get_primary_keys(new PrimaryKeysRequest(databaseName, tableName))
                .getPrimaryKeys());
      }

      @Override
      public ImmutableList<? extends TBase<?, ?>> getRawForeignKeys() throws Exception {
        return ImmutableList.copyOf(
            client
                .get_foreign_keys(
                    new ForeignKeysRequest(
                        /*parent_db_name=*/ null,
                        /*parent_tbl_name=*/ null,
                        databaseName,
                        tableName))
                .getForeignKeys());
      }

      @Override
      public ImmutableList<? extends TBase<?, ?>> getRawUniqueConstraints() throws Exception {
        return ImmutableList.copyOf(
            client
                .get_unique_constraints(
                    new UniqueConstraintsRequest(table.catName, databaseName, tableName))
                .getUniqueConstraints());
      }

      @Override
      public ImmutableList<? extends TBase<?, ?>> getRawNonNullConstraints() throws Exception {
        return ImmutableList.copyOf(
            client
                .get_not_null_constraints(
                    new NotNullConstraintsRequest(table.catName, databaseName, tableName))
                .getNotNullConstraints());
      }

      @Override
      public ImmutableList<? extends TBase<?, ?>> getRawDefaultConstraints() throws Exception {
        return ImmutableList.copyOf(
            client
                .get_default_constraints(
                    new DefaultConstraintsRequest(table.catName, databaseName, tableName))
                .getDefaultConstraints());
      }

      @Override
      public ImmutableList<? extends TBase<?, ?>> getRawCheckConstraints() throws Exception {
        return ImmutableList.copyOf(
            client
                .get_check_constraints(
                    new CheckConstraintsRequest(table.catName, databaseName, tableName))
                .getCheckConstraints());
      }

      @Override
      public ImmutableList<? extends TBase<?, ?>> getRawTableStatistics() throws Exception {
        ImmutableList columnNames =
            getFields().stream().map(Field::getFieldName).collect(toImmutableList());
        return ImmutableList.copyOf(
            client
                .get_table_statistics_req(
                    new TableStatsRequest(
                        databaseName, tableName, columnNames, /* engine= */ "hive"))
                .getTableStats());
      }

      @Override
      public ImmutableList<? extends TBase<?, ?>> getRawPartitions() throws Exception {
        return ImmutableList.copyOf(
            client.get_partitions(databaseName, tableName, /* max_parts= */ (short) -1));
      }
    };
  }

  @Nonnull
  @Override
  public List<? extends Function> getFunctions() throws Exception {
    com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.GetAllFunctionsResponse
        allFunctions = client.get_all_functions();
    List<Function> out = new ArrayList<>();
    for (com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.Function function :
        allFunctions.getFunctions()) {
      out.add(
          new Function() {
            @CheckForNull
            @Override
            public String getDatabaseName() {
              return (function.isSetDbName() ? function.getDbName() : null);
            }

            @CheckForNull
            @Override
            public String getFunctionName() {
              return (function.isSetFunctionName() ? function.getFunctionName() : null);
            }

            @CheckForNull
            @Override
            public String getType() {
              return (function.isSetFunctionType() ? function.getFunctionType().toString() : null);
            }

            @CheckForNull
            @Override
            public String getClassName() {
              return (function.isSetClassName() ? function.getClassName() : null);
            }

            @CheckForNull
            @Override
            public String getOwner() {
              return function.getOwnerName();
            }

            @CheckForNull
            @Override
            public Integer getOwnerType() {
              return (function.isSetOwnerType() ? function.getOwnerType().getValue() : null);
            }

            @CheckForNull
            @Override
            public Integer getCreateTime() {
              return function.getCreateTime();
            }
          });
    }
    return out;
  }

  @Override
  public ImmutableList<? extends TBase<?, ?>> getRawFunctions() throws Exception {
    return ImmutableList.copyOf(client.get_all_functions().getFunctions());
  }

  @Override
  public ImmutableList<? extends TBase<?, ?>> getRawResourcePlans() throws Exception {
    return ImmutableList.copyOf(
        client.get_all_resource_plans(new WMGetAllResourcePlanRequest()).getResourcePlans());
  }

  @Override
  public void close() throws IOException {
    try {
      client.shutdown();
    } catch (TException e) {
      throw new IOException("Unable to shutdown Thrift client.", e);
    }
  }

  @Override
  public ImmutableList<? extends TBase<?, ?>> getRawCatalogs() throws TException {
    GetCatalogsResponse catalogs = client.get_catalogs();
    return catalogs.getNames().stream()
        .map(
            catalogName -> {
              try {
                return client.get_catalog(new GetCatalogRequest(catalogName));
              } catch (TException e) {
                throw new IllegalStateException(e);
              }
            })
        .collect(toImmutableList());
  }
}
