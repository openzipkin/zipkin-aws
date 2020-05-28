/*
 * Copyright 2016-2020 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.storage.xray_udp;

import com.squareup.moshi.JsonWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import okio.Buffer;
import zipkin2.Span;

import static java.lang.Integer.parseInt;

/**
 * Encodes a Zipkin span into the JSON format expected by AWS' X-Ray daemon.
 */
final class UDPMessageEncoder {
  static final Logger logger = Logger.getLogger(UDPMessageEncoder.class.getName());

  static void writeJson(Span span, Buffer buffer) throws IOException {
    JsonWriter writer = JsonWriter.of(buffer);
    writer.beginObject();
    writer.name("trace_id");
    writer.value(new Buffer()
        .writeByte('"')
        .writeByte('1') // version
        .writeByte('-')
        .writeUtf8(span.traceId(), 0, 8) // 32-bit epoch seconds
        .writeByte('-')
        .writeUtf8(span.traceId(), 8, 32) // 96-bit trace ID
        .writeByte('"'));
    if (span.parentId() != null) writer.name("parent_id").value(span.parentId());
    writer.name("id").value(span.id());
    if (span.kind() == null) {
      // Spans without a kind should be internal operations in the service (for example an
      // hystrix command). The X-Ray documentation says that the subsegment name for
      // internal operations should be the name of the function invoked.
      // See the subsegment section at
      // https://docs.aws.amazon.com/xray/latest/devguide/xray-api-segmentdocuments.html

      // Subsegments are never root spans. Make sure root internal spans aren't marked as subsegment
      if (span.parentId() != null) writer.name("type").value("subsegment");
      writer.name("name").value(span.name() == null ? "unknown" : span.name());
    } else if (span.kind() == Span.Kind.CLIENT || span.kind() == Span.Kind.PRODUCER) {
      // Subsegments are never root spans. Make sure root client spans aren't marked as subsegment
      if (span.parentId() != null) writer.name("type").value("subsegment");
      writer.name("namespace").value("remote");

      // For the remote subsegment name, use a fallback model:
      //
      // Start with the remote service name
      //  fallback to the hostname (ex "http.host" tag if available)
      //  fallback to the span name (because it is already hinted as low cardinality)
      //  fallback to "unknown", so that instrumentation can be detected
      //
      // When users report a problem, such as a graph with too much cardinality, tell them to
      // configure a valid remote service name. This could happen if the hostname is ephemeral, for
      // example.
      //
      // This could be achieved by using things like HttpTracing.clientOf, or by wrapping the
      // reporter to choose a remote service name based on other data.
      String name = span.remoteServiceName();
      if (name == null) name = span.tags().get("http.host");
      if (name == null) name = span.name();
      if (name == null) name = "unknown";
      writer.name("name").value(name);
    } else {
      writer.name("name").value(span.localServiceName());
    }
    // override with the user remote tag
    if (span.tags().get("xray.namespace") != null) {
      writer.name("namespace").value(span.tags().get("xray.namespace"));
    }

    if (span.timestamp() != null) {
      writer.name("start_time").value(span.timestamp() / 1_000_000.0D);
      if (span.duration() != null) {
        writer.name("end_time").value((span.timestamp() + span.duration()) / 1_000_000.0D);
      } else {
        writer.name("in_progress").value(true);
      }
    }

    // http section
    String httpRequestMethod = null, httpRequestUrl = null;
    Integer httpResponseStatus = null;
    // sql section
    String sqlUrl = null, sqlPreparation = null, sqlDatabaseType = null, sqlDatabaseVersion = null;
    String sqlDriverVersion = null, sqlUser = null, sqlSanitizedQuery = null;
    // aws section
    String awsOperation = null,
        awsAccountId = null,
        awsRegion = null,
        awsRequestId = null,
        awsQueueUrl = null;
    String awsTableName = null;
    // aws.ec2 section
    String ec2AvailabilityZone = null,
        ec2InstanceId = null;
    // cause section
    String causeWorkingDirectory = null, causeExceptions = null;
    boolean http = false, sql = false, aws = false, cause = false;

    Map<String, String> annotations = new LinkedHashMap<>();
    Map<String, String> metadata = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : span.tags().entrySet()) {
      if (entry.getKey().startsWith("http.")) {
        http = true;
        switch (entry.getKey()) {
          case "http.method":
            httpRequestMethod = entry.getValue();
            continue;
          case "http.url":
            httpRequestUrl = entry.getValue();
            continue;
          case "http.status_code":
            httpResponseStatus = parseInt(entry.getValue());
            continue;
        }
      }
      if (entry.getKey().startsWith("sql.")) {
        sql = true;
        switch (entry.getKey()) {
          case "sql.url":
            sqlUrl = entry.getValue();
            continue;
          case "sql.preparation":
            sqlPreparation = entry.getValue();
            continue;
          case "sql.database_type":
            sqlDatabaseType = entry.getValue();
            continue;
          case "sql.database_version":
            sqlDatabaseVersion = entry.getValue();
            continue;
          case "sql.driver_version":
            sqlDriverVersion = entry.getValue();
            continue;
          case "sql.user":
            sqlUser = entry.getValue();
            continue;
          case "sql.sanitized_query":
            sqlSanitizedQuery = entry.getValue();
            continue;
        }
      }

      if (entry.getKey().startsWith("aws.")) {
        aws = true;
        switch (entry.getKey()) {
          case "aws.operation":
            awsOperation = entry.getValue();
            continue;
          case "aws.account_id":
            awsAccountId = entry.getValue();
            continue;
          case "aws.region":
            awsRegion = entry.getValue();
            continue;
          case "aws.request_id":
            awsRequestId = entry.getValue();
            continue;
          case "aws.queue_url":
            awsQueueUrl = entry.getValue();
            continue;
          case "aws.table_name":
            awsTableName = entry.getValue();
            continue;
          case "aws.ec2.availability_zone":
            ec2AvailabilityZone = entry.getValue();
            continue;
          case "aws.ec2.instance_id":
            ec2InstanceId = entry.getValue();
            continue;
        }
      }

      if (entry.getKey().startsWith("cause.")) {
        cause = true;
        switch (entry.getKey()) {
          case "cause.working_directory":
            causeWorkingDirectory = entry.getValue();
            continue;
          case "cause.exceptions":
            causeExceptions = entry.getValue();
            continue;
        }
      }
      String key = entry.getKey().replace('.', '_');
      if (entry.getValue().length() < 250) {
        annotations.put(key, entry.getValue());
      } else {
        metadata.put(key, entry.getValue());
      }
    }

    if (http) {
      if (httpRequestMethod == null) {
        httpRequestMethod = span.name(); // TODO validate
      }
      writer.name("http");
      writer.beginObject();
      if (httpRequestMethod != null || httpRequestUrl != null) {
        writer.name("request");
        writer.beginObject();
        if (httpRequestMethod != null) {
          writer.name("method").value(httpRequestMethod.toUpperCase());
        }
        if (httpRequestUrl != null) {
          writer.name("url").value(httpRequestUrl);
        }
        writer.endObject();
      }
      if (httpResponseStatus != null) {
        writer.name("response");
        writer.beginObject();
        writer.name("status").value(httpResponseStatus);
        writer.endObject();
      }
      writer.endObject();
    }

    Integer errorStatus = httpResponseStatus;

    if (errorStatus != null) {
      if (errorStatus == 429) {
        writer.name("throttle").value(true);
      } else if (errorStatus >= 500) {
        writer.name("fault").value(true);
      } else if (errorStatus >= 400) writer.name("error").value(true);
    }

    if (sql) {
      writer.name("sql");
      writer.beginObject();
      if (sqlUrl != null) writer.name("url").value(sqlUrl);
      if (sqlPreparation != null) writer.name("preparation").value(sqlPreparation);
      if (sqlDatabaseType != null) writer.name("database_type").value(sqlDatabaseType);
      if (sqlDatabaseVersion != null) writer.name("database_version").value(sqlDatabaseVersion);
      if (sqlDriverVersion != null) writer.name("driver_version").value(sqlDriverVersion);
      if (sqlUser != null) writer.name("user").value(sqlUser);
      if (sqlSanitizedQuery != null) writer.name("sanitized_query").value(sqlUser);
      writer.endObject();
    }

    if (aws) {
      writer.name("aws");
      writer.beginObject();
      if (awsOperation != null) writer.name("operation").value(awsOperation);
      if (awsAccountId != null) writer.name("account_id").value(awsAccountId);
      if (awsRegion != null) writer.name("region").value(awsRegion);
      if (awsRequestId != null) writer.name("request_id").value(awsRequestId);
      if (awsQueueUrl != null) writer.name("queue_url").value(awsQueueUrl);
      if (awsTableName != null) writer.name("table_name").value(awsTableName);
      if (ec2AvailabilityZone != null || ec2InstanceId != null) {
        writer.name("ec2");
        writer.beginObject();
        if (ec2AvailabilityZone != null) writer.name("availability_zone").value(ec2AvailabilityZone);
        if (ec2InstanceId != null) writer.name("instance_id").value(ec2InstanceId);
        writer.endObject();
      }
      writer.endObject();
    }

    if (cause) {
      writer.name("cause");
      writer.beginObject();
      if (causeWorkingDirectory != null) {
        writer.name("working_directory").value(causeWorkingDirectory);
      }
      if (causeExceptions != null) {
        String s = "\"exceptions\" :";
        if (causeWorkingDirectory != null) s = "," + s;
        buffer.writeUtf8(s + causeExceptions);
      }
      writer.endObject();
    }

    if (!annotations.isEmpty()) {
      writer.name("annotations");
      writer.beginObject();
      if (httpRequestMethod != null
          && span.name() != null
          && !httpRequestMethod.equals(span.name())) {
        writer.name("operation").value(span.name());
      }
      for (Map.Entry<String, String> annotation : annotations.entrySet()) {
        writer.name(annotation.getKey()).value(annotation.getValue());
      }
      writer.endObject();
    }
    if (!metadata.isEmpty()) {
      writer.name("metadata");
      writer.beginObject();
      for (Map.Entry<String, String> metadatum : metadata.entrySet()) {
        writer.name(metadatum.getKey()).value(metadatum.getValue());
      }
      writer.endObject();
    }
    writer.endObject();
    writer.flush();
  }

  static byte[] encode(Span span) {
    try {
      // TODO: also sanity check first 8 chars are epoch seconds
      if (span.traceId().length() != 32) {
        if (logger.isLoggable(Level.FINE)) {
          logger.fine("span reported without a 128-bit trace ID" + span);
        }
        throw new IllegalStateException("Change the tracer to use 128-bit trace IDs");
      }
      Buffer buffer = new Buffer();
      buffer.writeUtf8("{\"format\": \"json\", \"version\": 1}\n");
      writeJson(span, buffer);
      return buffer.readByteArray();
    } catch (IOException e) {
      throw new AssertionError(e); // encoding error is a programming bug
    }
  }
}
