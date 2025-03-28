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
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.edwmigration.dumper.application.dumper.SummaryPrinter.joinSummaryDoubleLine;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.model.Cluster;
import com.amazonaws.services.redshift.model.DescribeClustersRequest;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.redshift.RedshiftClusterUsageMetricsTask.MetricDataPoint;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftRawLogsDumpFormat;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;

/** Extraction task to get Redshift time series metrics from AWS CloudWatch API. */
public class RedshiftClusterUsageMetricsTask extends AbstractAwsApiTask {

  protected static enum MetricName {
    CPUUtilization,
    PercentageDiskSpaceUsed
  }

  protected static enum MetricType {
    Average
  }

  @AutoValue
  protected abstract static class MetricConfig {

    public abstract MetricName name();

    public abstract MetricType type();

    public static MetricConfig create(MetricName name, MetricType type) {
      return new AutoValue_RedshiftClusterUsageMetricsTask_MetricConfig(name, type);
    }
  }

  @AutoValue
  protected abstract static class MetricDataPoint {

    public abstract Instant instant();

    public abstract Double value();

    public abstract MetricConfig metricConfig();

    public static MetricDataPoint create(Instant instant, Double value, MetricConfig metricConfig) {
      return new AutoValue_RedshiftClusterUsageMetricsTask_MetricDataPoint(
          instant, value, metricConfig);
    }
  }

  private static final String REDSHIFT_NAMESPACE = "AWS/Redshift";
  private static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

  private final ZonedDateTime currentTime;
  private final ZonedInterval interval;
  private final String zipEntryName;

  public RedshiftClusterUsageMetricsTask(
      AWSCredentialsProvider credentialsProvider,
      ZonedDateTime currentTime,
      ZonedInterval interval,
      String zipEntryName) {
    super(
        credentialsProvider,
        zipEntryName,
        RedshiftRawLogsDumpFormat.ClusterUsageMetrics.Header.class);
    this.interval = interval;
    this.currentTime = currentTime;
    this.zipEntryName = zipEntryName;
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws IOException {
    CSVFormat format = FORMAT.builder().setHeader(headerEnum).build();
    try (CsvRecordWriter writer = new CsvRecordWriter(sink, format, getName())) {
      AmazonRedshift client = redshiftApiClient();
      List<Cluster> clusters = client.describeClusters(new DescribeClustersRequest()).getClusters();
      for (Cluster item : clusters) {
        writeCluster(writer, item.getClusterIdentifier());
      }
    }
    return null;
  }

  private void writeCluster(CsvRecordWriter writer, String clusterId) throws IOException {
    TreeMap<Instant, String> cpuPoints = new TreeMap<>();
    TreeMap<Instant, String> diskPoints = new TreeMap<>();

    for (MetricDataPoint dataPoint : getCpuMetrics(clusterId)) {
      cpuPoints.put(dataPoint.instant(), dataPoint.value().toString());
      diskPoints.putIfAbsent(dataPoint.instant(), "");
    }

    for (MetricDataPoint dataPoint : getDiskMetrics(clusterId)) {
      cpuPoints.putIfAbsent(dataPoint.instant(), "");
      diskPoints.put(dataPoint.instant(), dataPoint.value().toString());
    }

    for (Instant key : cpuPoints.keySet()) {
      String cpuValue = cpuPoints.get(key);
      String diskValue = diskPoints.get(key);
      writer.handleRecord(clusterId, DATE_FORMAT.format(key), cpuValue, diskValue);
    }
  }

  private ImmutableList<MetricDataPoint> getMetricDataPoints(
      String clusterId, MetricConfig metricConfig) {
    AmazonCloudWatch client = cloudWatchApiClient();
    GetMetricStatisticsRequest request =
        new GetMetricStatisticsRequest()
            .withMetricName(metricConfig.name().name())
            .withStatistics(metricConfig.type().name())
            .withNamespace(REDSHIFT_NAMESPACE)
            .withDimensions(new Dimension().withName("ClusterIdentifier").withValue(clusterId))
            .withStartTime(Date.from(interval.getStartUTC().toInstant()))
            .withEndTime(Date.from(interval.getEndExclusiveUTC().toInstant()))
            .withPeriod((int) metricDataPeriod().getSeconds());

    GetMetricStatisticsResult result = client.getMetricStatistics(request);
    return result.getDatapoints().stream()
        .map(
            datapoint ->
                MetricDataPoint.create(
                    datapoint.getTimestamp().toInstant(),
                    getDatapointValue(metricConfig, datapoint),
                    metricConfig))
        .collect(toImmutableList());
  }

  private Double getDatapointValue(MetricConfig metricConfig, Datapoint datapoint) {
    switch (metricConfig.type()) {
      case Average:
        return datapoint.getAverage();
      default:
        return null;
    }
  }

  private ImmutableList<MetricDataPoint> getDiskMetrics(String clusterId) {
    MetricConfig config =
        MetricConfig.create(MetricName.PercentageDiskSpaceUsed, MetricType.Average);
    return getMetricDataPoints(clusterId, config);
  }

  private ImmutableList<MetricDataPoint> getCpuMetrics(String clusterId) {
    MetricConfig config = MetricConfig.create(MetricName.CPUUtilization, MetricType.Average);
    return getMetricDataPoints(clusterId, config);
  }

  /**
   * Returns available metric period based on the interval time.
   * https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html
   */
  private Duration metricDataPeriod() {
    ZonedDateTime start = interval.getStartUTC();
    if (start.isAfter(currentTime.minusDays(14))) {
      return Duration.ofMinutes(1);
    }
    if (start.isAfter(currentTime.minusDays(62))) {
      return Duration.ofMinutes(5);
    }
    return Duration.ofHours(1);
  }

  private String toCallDescription() {
    return "AmazonRedshift.describeClusters, AmazonCloudWatch.getMetricStatistics";
  }

  @Override
  public String describeSourceData() {
    return joinSummaryDoubleLine(
        "Write " + zipEntryName + " from AWS API request:", toCallDescription());
  }
}
