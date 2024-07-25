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

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.HdfsPermissionExtractionDumpFormat;
import java.io.IOException;
import java.io.Writer;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsContentSummaryTask extends AbstractTask<Void>
    implements HdfsPermissionExtractionDumpFormat.ContentSummary {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsContentSummaryTask.class);

  private final String clusterHost;
  private final int port;

  HdfsContentSummaryTask(@Nonnull ConnectorArguments args) {
    super(ZIP_ENTRY_NAME);
    Preconditions.checkNotNull(args, "Arguments was null.");
    clusterHost = Preconditions.checkNotNull(args.getHost(), "Host was null.");
    port = args.getPort(/* defaultPort= */ 8020);
  }

  @Override
  public String toString() {
    return format(
        "Write content summary of the top-level directories of a hdfs %s", getTargetPath());
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws IOException {
    LOG.info("clusterHost: {}", clusterHost);
    LOG.info("port: {}", port);

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://" + clusterHost + ":" + port + "/");
    FileSystem fs = FileSystem.get(conf);
    String hdfsPath = "/";
    FileStatus rootDir = fs.getFileStatus(new Path(hdfsPath));
    FileStatus[] topLevelFiles = fs.listStatus(rootDir.getPath());
    try (final Writer output = sink.asCharSink(UTF_8).openBufferedStream();
        final CSVPrinter csvPrinter = FORMAT.withHeader(Header.class).print(output)) {
      for (FileStatus file : topLevelFiles) {
        if (file.isDirectory()) {
          ContentSummary summary = fs.getContentSummary(file.getPath());
          long totalFileSize = summary.getLength();
          long totalNumberOfFiles = summary.getFileCount();
          long totalNumberOfDirectories = summary.getDirectoryCount();
          csvPrinter.printRecord(
              file.getPath().toUri().getPath(),
              totalFileSize,
              totalNumberOfDirectories + totalNumberOfFiles);
        }
      }
    }
    return null;
  }
}