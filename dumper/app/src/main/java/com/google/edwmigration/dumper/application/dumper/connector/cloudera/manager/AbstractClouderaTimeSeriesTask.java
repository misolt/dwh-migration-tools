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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

abstract class AbstractClouderaTimeSeriesTask extends AbstractClouderaManagerTask {
  private static final DateTimeFormatter isoDateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final int includedLastDays;
  private final TimeSeriesAggregation tsAggregation;

  public AbstractClouderaTimeSeriesTask(
      String targetPath, int includedLastDays, TimeSeriesAggregation tsAggregation) {
    super(targetPath);
    Preconditions.checkNotNull(tsAggregation, "TimeSeriesAggregation has not to be a null.");
    Preconditions.checkArgument(
        includedLastDays >= 1,
        "The chart has to include at least one day. Received " + includedLastDays + " days.");
    this.includedLastDays = includedLastDays;
    this.tsAggregation = tsAggregation;
  }

  protected JsonNode requestTimeSeriesChart(ClouderaManagerHandle handle, String query) {
    String timeSeriesUrl = handle.getApiURI().toString() + "/timeseries";
    String fromDate = buildISODateTime(includedLastDays);
    URI tsURI;
    try {
      URIBuilder uriBuilder = new URIBuilder(timeSeriesUrl);
      uriBuilder.addParameter("query", query);
      uriBuilder.addParameter("desiredRollup", tsAggregation.toString());
      uriBuilder.addParameter("mustUseDesiredRollup", "true");
      uriBuilder.addParameter("from", fromDate);
      tsURI = uriBuilder.build();
    } catch (URISyntaxException ex) {
      throw new TimeSeriesException(ex.getMessage(), ex);
    }

    CloseableHttpClient httpClient = handle.getHttpClient();
    JsonNode chartInJson;
    try (CloseableHttpResponse chart = httpClient.execute(new HttpGet(tsURI))) {
      int statusCode = chart.getStatusLine().getStatusCode();
      if (!isStatusCodeOK(statusCode)) {
        throw new TimeSeriesException(
            String.format(
                "Cloudera Error: Response status code is %d but 2xx is expected.", statusCode));
      }
      chartInJson = readJsonTree(chart.getEntity().getContent());
    } catch (IOException ex) {
      throw new TimeSeriesException(ex.getMessage(), ex);
    }
    return chartInJson;
  }

  private String buildISODateTime(int deltaInDays) {
    ZonedDateTime dateTime =
        ZonedDateTime.of(LocalDateTime.now().minusDays(deltaInDays), ZoneId.of("UTC"));
    return dateTime.format(isoDateTimeFormatter);
  }

  static class TimeSeriesException extends RuntimeException {
    /* Exception which should be returned if something goes wrong with timeseries API.
     *
     * Includes:
     * - unexpected HTTP status codes;
     * - response with invalid JSON format.
     */

    public TimeSeriesException(String message) {
      super(message);
    }

    public TimeSeriesException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  enum TimeSeriesAggregation {
    RAW,
    TEN_MINUTELY,
    HOURLY,
    SIX_HOURLY,
    DAILY,
    WEEKLY,
  }
}
