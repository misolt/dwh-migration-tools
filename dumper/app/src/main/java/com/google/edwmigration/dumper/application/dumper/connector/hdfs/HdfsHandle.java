/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.hdfs;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HdfsHandle implements Handle {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsHandle.class);

  private final String clusterHost;
  private final int port;
  private final DistributedFileSystem dfs;

  HdfsHandle(@Nonnull ConnectorArguments args) throws IOException {
    Preconditions.checkNotNull(args, "Arguments was null.");
    clusterHost = args.getHostOrDefault();
    port = args.getPort(/* defaultPort= */ 8020);

    LOG.info("clusterHost: '{}'", clusterHost);
    LOG.info("port: '{}'", port);

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://" + clusterHost + ":" + port + "/");
    FileSystem fs = FileSystem.get(conf);
    checkArgument(fs instanceof DistributedFileSystem, "Not a DistributedFileSystem");
    dfs = (DistributedFileSystem) fs;
  }

  DistributedFileSystem getDfs() {
    return dfs;
  }

  @Override
  public void close() throws IOException {
    // No need to close dfs - it's read only accessed by multiple Tasks
    // and then the process exits.
  }
}
