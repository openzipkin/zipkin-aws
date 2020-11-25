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
  Span serverSpan =
      Span.newBuilder()
          .traceId("1234567890abcdef1234567890abcdef")
          .id("1234567890abcdef")
          .kind(Span.Kind.SERVER)
          .name("test-cemo")
          .build();

  @Test
  public void writeJson_server_isSegment() throws Exception {
    Span span = serverSpan;

    assertThat(writeJson(span))
        .isEqualTo(
            "{\"trace_id\":\"1-12345678-90abcdef1234567890abcdef\",\"id\":\"1234567890abcdef\",\"aws\":{\"xray\":{\"sdk\":\"Zipkin\"}}}");
  }

  @Test
  public void writeJson_origin_no_default() throws Exception {
    Span span =
        serverSpan
            .toBuilder()
            .build();

    String json = writeJson(span);
    assertThat(readString(json, "origin")).isNull();
  }

  @Test
  public void writeJson_origin_custom() throws Exception {
    Span span =
        serverSpan
            .toBuilder()
            .putTag("aws.origin", "AWS::EC2::Instance")
            .build();

    String json = writeJson(span);
    assertThat(readString(json, "origin")).isEqualTo("AWS::EC2::Instance");
    assertThat(readString(json, "annotations.aws_origin")).isNull();
  }

  @Test
  public void writeJson_server_localEndpointIsName() throws Exception {
    Span span =
        serverSpan
            .toBuilder()
            .localEndpoint(Endpoint.newBuilder().serviceName("master").build())
            .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("master");
  }

  @Test
  public void writeJson_client_isRemote() throws Exception {
    Span span = serverSpan.toBuilder().kind(Span.Kind.CLIENT).build();

    String json = writeJson(span);
    assertThat(readString(json, "namespace")).isEqualTo("remote");
  }

  @Test
  public void writeJson_client_child_isRemoteSubsegment() throws Exception {
    Span span = serverSpan.toBuilder().parentId('1').kind(Span.Kind.CLIENT).build();

    String json = writeJson(span);
    assertThat(readString(json, "type")).isEqualTo("subsegment");
    assertThat(readString(json, "namespace")).isEqualTo("remote");
  }

  @Test
  public void writeJson_custom_nameIsUnknown() throws Exception {
    Span span = serverSpan.toBuilder()
        .kind(null)
        .name(null)
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("unknown");
  }

  @Test
  public void writeJson_custom_nameIsName() throws Exception {
    Span span = serverSpan.toBuilder()
        .kind(null)
        .name("hystrix")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("hystrix");
  }

  @Test
  public void writeJson_client_nameIsUnknown() throws Exception {
    Span span = Span.newBuilder()
        .traceId(serverSpan.traceId()).id("b")
        .kind(Span.Kind.CLIENT)
        .remoteEndpoint(Endpoint.newBuilder().ip("1.2.3.4").build())
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("unknown");
  }

  @Test
  public void writeJson_client_nameIsHost() throws Exception {
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.CLIENT)
        .name("get /")
        .putTag("http.host", "facebook.com")
        .localEndpoint(Endpoint.newBuilder().serviceName("master").build())
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("facebook.com");
  }

  @Test
  public void writeJson_client_nameIsName() throws Exception {
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.CLIENT)
        .name("get /")
        .localEndpoint(Endpoint.newBuilder().serviceName("master").build())
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("get /");
  }

  @Test
  public void writeJson_client_nameIsUnknownWhenNameNull() throws Exception {
    Span span = Span.newBuilder()
        .traceId(serverSpan.traceId()).id("b")
        .kind(Span.Kind.CLIENT)
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("unknown");
  }

  @Test
  public void writeJson_client_remoteEndpointIsName() throws Exception {
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.CLIENT)
        .remoteEndpoint(Endpoint.newBuilder().serviceName("master").build())
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("master");
  }

  @Test
  public void writeJson_http() throws Exception {
    Span span =
        serverSpan
            .toBuilder()
            .name("get")
            .putTag("http.url", "http://foo/bar")
            .putTag("http.status_code", "200")
            .build();

    String json = writeJson(span);
    assertThat(readMap(json, "http.request"))
        .containsExactly(entry("method", "GET"), entry("url", "http://foo/bar"));
    assertThat(readMap(json, "http.response")).containsExactly(entry("status", 200));
  }

  @Test
  public void writeJson_http_clientError() throws Exception {
    Span span =
        serverSpan
            .toBuilder()
            .name("get")
            .putTag("http.url", "http://foo/bar")
            .putTag("http.status_code", "409")
            .build();

    String json = writeJson(span);
    assertThat(readBoolean(json, "error")).isTrue();
    assertThat(readBoolean(json, "fault")).isNull();
  }

  @Test
  public void writeJson_http_serverError() throws Exception {
    Span span =
        serverSpan
            .toBuilder()
            .name("get")
            .putTag("http.url", "http://foo/bar")
            .putTag("http.status_code", "500")
            .build();

    String json = writeJson(span);
    assertThat(readBoolean(json, "error")).isNull();
    assertThat(readBoolean(json, "fault")).isTrue();
  }

  @Test
  public void writeJson_sql() throws Exception {
    Span span = serverSpan.toBuilder().putTag("sql.url", "jdbc:test").build();

    String json = writeJson(span);
    assertThat(readMap(json, "sql")).containsExactly(entry("url", "jdbc:test"));
  }

  @Test
  public void writeJson_aws() throws Exception {
    Span span =
        serverSpan
            .toBuilder()
            .putTag("aws.region", "reg1")
            .putTag("aws.table_name", "table1")
            .build();

    String json = writeJson(span);
    Map<String, Object> map = readMap(json, "aws");
    assertThat(map).hasSize(3);
    assertThat(map).containsEntry("region", "reg1");
    assertThat(map).containsEntry("table_name", "table1");
    assertThat(map.get("xray")).isInstanceOfSatisfying(Map.class, xray -> {
      assertThat(xray).containsExactly(entry("sdk", "Zipkin"));
    });
  }

  @Test
  public void writeJson_sdkProvided() throws Exception {
    Span span =
        serverSpan
            .toBuilder()
            .putTag("aws.xray.sdk", "Zipkin Brave")
            .build();

    String json = writeJson(span);
    assertThat(readMap(json, "aws").get("xray")).isInstanceOfSatisfying(Map.class, xray -> {
      assertThat(xray).containsExactly(entry("sdk", "Zipkin Brave"));
    });
  }

  @Test
  public void writeJson_aws_ec2() throws Exception {
    Span span =
            serverSpan
                    .toBuilder()
                    .putTag("aws.ec2.availability_zone", "us-west-2c")
                    .putTag("aws.ec2.instance_id", "i-0b5a4678fc325bg98")
                    .build();

    String json = writeJson(span);
    assertThat(readMap(json, "aws.ec2"))
            .containsExactly(
                    entry("availability_zone", "us-west-2c"),
                    entry("instance_id", "i-0b5a4678fc325bg98"));
  }

  String writeJson(Span span) throws IOException {
    Buffer buffer = new Buffer();
    UDPMessageEncoder.writeJson(span, buffer);
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
