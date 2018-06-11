/**
 * Copyright 2016-2018 The OpenZipkin Authors
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

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import java.io.IOException;
import java.util.Map;
import okio.Buffer;
import org.junit.Test;
import zipkin2.Endpoint;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;

public class UDPMessageEncoderTest {
  Span serverSpan = Span.newBuilder()
      .traceId("1234567890abcdef1234567890abcdef")
      .id("1234567890abcdef")
      .kind(Span.Kind.SERVER)
      .name("test-cemo")
      .build();

  @Test public void writeJson_server_isSegment() throws Exception {
    Span span = serverSpan;

    assertThat(writeJson(span)).isEqualTo(
        "{\"trace_id\":\"1-12345678-90abcdef1234567890abcdef\",\"id\":\"1234567890abcdef\"}"
    );
  }

  @Test public void writeJson_server_localEndpointIsName() throws Exception {
    Span span = serverSpan.toBuilder()
        .localEndpoint(Endpoint.newBuilder().serviceName("master").build())
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("master");
  }

  @Test public void writeJson_client_isRemoteSubsegment() throws Exception {
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.CLIENT)
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "type")).isEqualTo("subsegment");
    assertThat(readString(json, "namespace")).isEqualTo("remote");
  }

  @Test public void writeJson_custom_nameIsUnknown() throws Exception {
    Span span = serverSpan.toBuilder()
        .kind(null)
        .name(null)
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("unknown");
  }

  @Test public void writeJson_custom_nameIsName() throws Exception {
    Span span = serverSpan.toBuilder()
        .kind(null)
        .name("hystrix")
        .build();

    String json = writeJson(span, true);
    assertThat(readString(json, "name")).isEqualTo("hystrix");
  }

  @Test public void writeJson_client_nameIsUnknown() throws Exception {
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.CLIENT)
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("unknown");
  }

  @Test public void writeJson_client_localEndpointIsName() throws Exception {
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.CLIENT)
        .localEndpoint(Endpoint.newBuilder().serviceName("master").build())
        .build();

    String json = writeJson(span, true);
    assertThat(readString(json, "name")).isEqualTo("master");
  }

  @Test public void writeJson_client_nameIsUnknownWhenLocalServiceNameNull() throws Exception {
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.CLIENT)
        .build();

    String json = writeJson(span, true);
    assertThat(readString(json, "name")).isEqualTo("unknown");
  }

  @Test public void writeJson_client_remoteEndpointIsName() throws Exception {
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.CLIENT)
        .remoteEndpoint(Endpoint.newBuilder().serviceName("master").build())
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("master");
  }

  @Test public void writeJson_http() throws Exception {
    Span span = serverSpan.toBuilder()
        .name("get")
        .putTag("http.url", "http://foo/bar")
        .putTag("http.status_code", "200")
        .build();

    String json = writeJson(span);
    assertThat(readMap(json, "http.request")).containsExactly(
        entry("method", "GET"),
        entry("url", "http://foo/bar")
    );
    assertThat(readMap(json, "http.response")).containsExactly(
        entry("status", 200)
    );
  }

  @Test public void writeJson_http_clientError() throws Exception {
    Span span = serverSpan.toBuilder()
        .name("get")
        .putTag("http.url", "http://foo/bar")
        .putTag("http.status_code", "409")
        .build();

    String json = writeJson(span);
    assertThat(readBoolean(json, "error")).isTrue();
    assertThat(readBoolean(json, "fault")).isNull();
  }

  @Test public void writeJson_http_serverError() throws Exception {
    Span span = serverSpan.toBuilder()
        .name("get")
        .putTag("http.url", "http://foo/bar")
        .putTag("http.status_code", "500")
        .build();

    String json = writeJson(span);
    assertThat(readBoolean(json, "error")).isNull();
    assertThat(readBoolean(json, "fault")).isTrue();
  }

  @Test public void writeJson_sql() throws Exception {
    Span span = serverSpan.toBuilder()
        .putTag("sql.url", "jdbc:test")
        .build();

    String json = writeJson(span);
    assertThat(readMap(json, "sql")).containsExactly(
        entry("url", "jdbc:test")
    );
  }

  @Test public void writeJson_aws() throws Exception {
    Span span = serverSpan.toBuilder()
        .putTag("aws.region", "reg1")
        .putTag("aws.table_name", "table1")
        .build();

    String json = writeJson(span);
    assertThat(readMap(json, "aws")).containsExactly(
        entry("region", "reg1"),
        entry("table_name", "table1")
    );
  }

  String writeJson(Span span) throws IOException {
    return writeJson(span, false);
  }

  String writeJson(Span span, boolean useLocalServiceNameWhenRemoteIsMissing) throws IOException {
    Buffer buffer = new Buffer();
    UDPMessageEncoder.writeJson(span, buffer, useLocalServiceNameWhenRemoteIsMissing);
    return buffer.readUtf8();
  }

  static Boolean readBoolean(String json, String jsonPath) {
    return tryRead(json, jsonPath);
  }

  static String readString(String json, String jsonPath) {
    return tryRead(json, jsonPath);
  }

  static Map<String, Object> readMap(String json, String jsonPath) {
    return tryRead(json, jsonPath);
  }

  static <T> T tryRead(String json, String jsonPath) {
    try {
      return JsonPath.compile(jsonPath).read(json);
    } catch (PathNotFoundException e) {
      return null;
    }
  }
}
