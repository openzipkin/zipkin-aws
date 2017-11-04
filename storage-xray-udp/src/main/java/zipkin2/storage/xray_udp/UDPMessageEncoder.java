/**
 * Copyright 2016-2017 The OpenZipkin Authors
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

final class UDPMessageEncoder {
  static final Logger logger = Logger.getLogger(UDPMessageEncoder.class.getName());

  static byte[] encode(Span span) {
    try {
      return doEncode(span);
    } catch (IOException e) {
      throw new AssertionError(e); // encoding error is a programming bug
    }
  }

  static byte[] doEncode(Span span) throws IOException {
    if (span.traceId().length() != 32) { // TODO: also sanity check first 8 chars are epoch seconds
      if (logger.isLoggable(Level.FINE)) {
        logger.fine("span reported without a 128-bit trace ID" + span);
      }
      throw new IllegalStateException("Change the tracer to use 128-bit trace IDs");
    }
    Buffer buffer = new Buffer();
    buffer.writeUtf8("{\"format\": \"json\", \"version\": 1}\n");
    JsonWriter writer = JsonWriter.of(buffer);
    writer.beginObject();
    writer.name("trace_id").value(new StringBuilder()
        .append("1-")
        .append(span.traceId(), 0, 8)
        .append('-')
        .append(span.traceId(), 8, 32).toString());
    if (span.parentId() != null) writer.name("parent_id").value(span.parentId());
    writer.name("id").value(span.id());
    if (span.kind() == null
        || span.kind() != Span.Kind.SERVER && span.kind() != Span.Kind.CONSUMER) {
      writer.name("type").value("subsegment");
      if (span.kind() != null) writer.name("namespace").value("remote");
      writer.name("name").value(span.remoteServiceName() == null ? "" : span.remoteServiceName());
    }else{
      writer.name("name").value(span.localServiceName());
    }

    if (span.timestamp() != null) {
      writer.name("start_time").value(span.timestamp() / 1_000_000.0D);
      if (span.duration() != null) {
        writer.name("end_time").value((span.timestamp() + span.duration()) / 1_000_000.0D);
      } else {
        writer.name("in_progress").value(true);
      }
    }

    String httpRequestMethod = null, httpRequestUrl = null;
    Integer httpResponseStatus = null;
    boolean http = false;

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
            httpResponseStatus = Integer.parseInt(entry.getValue());
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

    if (!annotations.isEmpty()) {
      writer.name("annotations");
      writer.beginObject();
      if (httpRequestMethod != null && span.name() != null && !httpRequestMethod.equals(
          span.name())) {
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
    return buffer.readByteArray();
  }
}
