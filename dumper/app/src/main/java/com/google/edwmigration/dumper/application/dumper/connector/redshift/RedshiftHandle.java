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
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class RedshiftHandle implements Handle {

  private Optional<AmazonCloudWatch> cloudWatchClient = Optional.empty();
  private Optional<AmazonRedshift> redshiftClient = Optional.empty();

  @Override
  public void close() {}

  AmazonCloudWatch getCloudWatchClient() {
    return cloudWatchClient.get();
  }

  AmazonRedshift getRedshiftClient() {
    return redshiftClient.get();
  }

  @Nonnull
  public RedshiftHandle withCloudWatchApiClient(AmazonCloudWatch cloudWatchClient) {
    this.cloudWatchClient = Optional.of(cloudWatchClient);
    return this;
  }

  @Nonnull
  public RedshiftHandle withRedshiftApiClient(AmazonRedshift redshiftClient) {
    this.redshiftClient = Optional.of(redshiftClient);
    return this;
  }
}
