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
package com.google.edwmigration.dumper.application.dumper.task;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.MetadataDumperConstants;
import java.nio.charset.StandardCharsets;
import org.anarres.jdiagnostics.ProductMetadata;

/** @author shevek */
public class VersionTask extends AbstractTask<Void> {

  public VersionTask(String targetPath) {
    super(targetPath);
  }

  public VersionTask() {
    this(MetadataDumperConstants.VERSION_ZIP_ENTRY_NAME);
  }

  @Override
  public TaskCategory getCategory() {
    return TaskCategory.INFORMATIONAL;
  }

  @Override
  protected Void doRun(TaskRunContext context, ByteSink sink, Handle handle) throws Exception {
    sink.asCharSink(StandardCharsets.UTF_8).write(String.valueOf(new ProductMetadata()));
    return null;
  }

  @Override
  public String describeSourceData() {
    return "from product version information.";
  }
}
