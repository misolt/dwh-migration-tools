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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.ClouderaMetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.meta.MetaHandle;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

public class HadoopInitializerTask extends AbstractTask<Void> {

  public HadoopInitializerTask() {
    super("hadoop-initializer.txt", /* createTarget= */ false);
  }

  @CheckForNull
  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    MetaHandle metaHandle = (MetaHandle) handle;
    metaHandle.initializeConnector(
        ClouderaMetadataConnector.CONNECTOR_NAME,
        new ConnectorArguments("--connector", ClouderaMetadataConnector.CONNECTOR_NAME));
    return null;
  }
}