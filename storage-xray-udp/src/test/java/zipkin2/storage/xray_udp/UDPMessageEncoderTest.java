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
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import okio.Buffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import zipkin2.Endpoint;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;

public class UDPMessageEncoderTest {
  Span serverSpan =
      Span.newBuilder()
          .traceId(TRACE_ID)
          .id("1234567890abcdef")
          .kind(Span.Kind.SERVER)
          .name("test-cemo")
          .build();

  private static final String TRACE_ID = "1234567890abcdef1234567890abcdef";
  private static final List<String> TRACE_IDS = Arrays.asList(TRACE_ID);

  @Before
  public void setup() throws Exception {
    UDPMessageEncoder.EPOCH_CACHE.invalidateAll(TRACE_IDS);
  }

  @After
  public void teardown() throws Exception {
    removeEnvAll();
  }

  @Test
  public void writeJson_server_isSegment() throws Exception {
    long currentTimeMillis = System.currentTimeMillis();
    String epoch = Long.toHexString(currentTimeMillis / 1000);
    double startTime = currentTimeMillis / 1_000.0D;
    Span span = serverSpan
        .toBuilder()
        .timestamp(currentTimeMillis * 1000)
        .build();

    assertThat(writeJson(span))
        .isEqualTo(
            String.format(
                "{\"trace_id\":\"1-%s-90abcdef1234567890abcdef\",\"id\":\"1234567890abcdef\",\"start_time\":%s,\"in_progress\":true,\"origin\":\"ServiceMesh::Istio\"}",
                epoch, startTime));
  }

  @Test
  public void writeJson_origin_default() throws Exception {
    Span span =
        serverSpan
            .toBuilder()
            .build();

    String json = writeJson(span);
    assertThat(readString(json, "origin")).isEqualTo("ServiceMesh::Istio");
  }

  @Test
  public void writeJson_origin_custom() throws Exception {
    updateEnv("AWS_XRAY_ORIGIN", "AWS::EC2::Instance");
    Span span =
        serverSpan
            .toBuilder()
            .build();

    String json = writeJson(span);
    assertThat(readString(json, "origin")).isEqualTo("AWS::EC2::Instance");
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
    assertThat(readMap(json, "aws"))
        .containsExactly(entry("region", "reg1"), entry("table_name", "table1"));
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

  @Test
  public void writeJson_client_remoteServiceNameContainsAsterisk_no_replacement() throws Exception {
    removeEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.CLIENT)
        .remoteEndpoint(Endpoint.newBuilder().serviceName("a.*.b.*").build())
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("a.*.b.*");
  }

  @Test
  public void writeJson_client_remoteServiceNameContainsAsterisk_replacement() throws Exception {
    updateEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR", "&");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.CLIENT)
        .remoteEndpoint(Endpoint.newBuilder().serviceName("a.*.b.*").build())
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("a.&.b.&");
  }

  @Test
  public void writeJson_client_httpHostContainsAsterisk_no_replacement() throws Exception {
    removeEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.CLIENT)
        .putTag("http.host", "a*.com")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("a*.com");
  }

  @Test
  public void writeJson_client_httpHostContainsAsterisk_replacement() throws Exception {
    updateEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR", ":");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.CLIENT)
        .putTag("http.host", "a*.com")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("a:.com");
  }

  @Test
  public void writeJson_client_nameContainsAsterisk_no_replacement() throws Exception {
    removeEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.CLIENT)
        .name("http://localhost:2345/product/*")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("http://localhost:2345/product/*");
  }

  @Test
  public void writeJson_client_nameContainsAsterisk_replacement() throws Exception {
    updateEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR", "#");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.CLIENT)
        .name("http://localhost:2345/product/*")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("http://localhost:2345/product/#");
  }

  @Test
  public void writeJson_producer_remoteServiceNameContainsAsterisk_no_replacement()
      throws Exception {
    removeEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.PRODUCER)
        .remoteEndpoint(
            Endpoint.newBuilder().serviceName("http://localhost:2345/product/*").build())
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("http://localhost:2345/product/*");
  }

  @Test
  public void writeJson_producer_remoteServiceNameContainsAsterisk_replacement() throws Exception {
    updateEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR", "%");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.PRODUCER)
        .remoteEndpoint(
            Endpoint.newBuilder().serviceName("http://localhost:2345/product/*").build())
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("http://localhost:2345/product/%");
  }

  @Test
  public void writeJson_producer_httpHostContainsAsterisk_no_replacement() throws Exception {
    removeEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.PRODUCER)
        .putTag("http.host", "a*.com")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("a*.com");
  }

  @Test
  public void writeJson_producer_httpHostContainsAsterisk_replacement() throws Exception {
    updateEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR", ":");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.PRODUCER)
        .putTag("http.host", "a*.com")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("a:.com");
  }

  @Test
  public void writeJson_producer_nameContainsAsterisk_no_replacement() throws Exception {
    removeEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.PRODUCER)
        .name("http://localhost:2345/product/*")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("http://localhost:2345/product/*");
  }

  @Test
  public void writeJson_producer_nameContainsAsterisk_replacement() throws Exception {
    updateEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR", "_");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.PRODUCER)
        .name("http://localhost:2345/product/*")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("http://localhost:2345/product/_");
  }

  @Test
  public void writeJson_custom_nameContainsAsterisk_no_replacement() throws Exception {
    removeEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR");
    Span span = serverSpan.toBuilder()
        .kind(null)
        .name("http://localhost:2345/product/*")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("http://localhost:2345/product/*");
  }

  @Test
  public void writeJson_custom_nameContainsAsterisk_replacement() throws Exception {
    updateEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR", "+");
    Span span = serverSpan.toBuilder()
        .kind(null)
        .name("http://localhost:2345/product/*")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("http://localhost:2345/product/+");
  }

  @Test
  public void writeJson_server_localServiceNameContainsAsterisk_no_replacement() throws Exception {
    removeEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.SERVER)
        .localEndpoint(Endpoint.newBuilder().serviceName("a.b.*.c").build())
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("a.b.*.c");
  }

  @Test
  public void writeJson_server_localServiceNameContainsAsterisk_replacement() throws Exception {
    updateEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR", "=");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.SERVER)
        .localEndpoint(Endpoint.newBuilder().serviceName("a.b.*.c").build())
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "name")).isEqualTo("a.b.=.c");
  }

  @Test
  public void writeJson_httpMethod_nameContainsAsterisk_no_replacement() throws Exception {
    removeEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.SERVER)
        .name("*.b.*.d")
        .putTag("http.url", "http://localhost:2345/product/*")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "http.request.method")).isEqualTo("*.B.*.D");
  }

  @Test
  public void writeJson_httpMethod_nameContainsAsterisk_replacement() throws Exception {
    updateEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR", "\\");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.SERVER)
        .name("*.b.*.d")
        .putTag("http.url", "http://localhost:2345/product/*")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "http.request.method")).isEqualTo("\\.B.\\.D");
  }

  @Test
  public void writeJson_annotations_operation_nameContainsAsterisk_no_replacement()
      throws Exception {
    removeEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.SERVER)
        .name("*.b.*.d")
        .putTag("http.url", "http://localhost:2345/product/*")
        .putTag("http.method", "GET")
        .putTag("environment", "test")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "annotations.operation")).isEqualTo("*.b.*.d");
  }

  @Test
  public void writeJson_annotations_operation_nameContainsAsterisk_replacement() throws Exception {
    updateEnv("AWS_XRAY_NAME_REPLACE_ASTERISK_WITH_CHAR", ".");
    Span span = serverSpan.toBuilder()
        .kind(Span.Kind.SERVER)
        .name("*.b.*.d")
        .putTag("http.url", "http://localhost:2345/product/*")
        .putTag("http.method", "GET")
        .putTag("environment", "test")
        .build();

    String json = writeJson(span);
    assertThat(readString(json, "annotations.operation")).isEqualTo("..b...d");
  }

  @Test
  public void getenv_no_env_default_return_string() {
    String ret = UDPMessageEncoder.getenv("RANDOM_ENV", "hello");
    assertThat(ret).isEqualTo("hello");
  }

  @Test
  public void getenv_valid_long_env_return_string() throws Exception {
    updateEnv("RANDOM_ENV", "world");
    String ret = UDPMessageEncoder.getenv("RANDOM_ENV", "hello");
    assertThat(ret).isEqualTo("world");
  }

  @Test
  public void getenv_no_env_default_return_long() {
    long ret = UDPMessageEncoder.getenv("RANDOM_ENV", 100);
    assertThat(ret).isEqualTo(100);
  }

  @Test
  public void getenv_invalid_long_env_default_return_long() throws Exception {
    updateEnv("RANDOM_ENV", "hello");
    long ret = UDPMessageEncoder.getenv("RANDOM_ENV", 100);
    assertThat(ret).isEqualTo(100);
  }

  @Test
  public void getenv_valid_long_env_return_long() throws Exception {
    updateEnv("RANDOM_ENV", "250");
    long ret = UDPMessageEncoder.getenv("RANDOM_ENV", 100);
    assertThat(ret).isEqualTo(250);
  }

  @Test
  public void getOrigin_no_env_default_return() {
    String ret = UDPMessageEncoder.getOrigin();
    assertThat(ret).isEqualTo("ServiceMesh::Istio");
  }

  @Test
  public void getOrigin_env_value_return() throws Exception {
    updateEnv("AWS_XRAY_ORIGIN", "AWS::EC2::Instance");
    String ret = UDPMessageEncoder.getOrigin();
    assertThat(ret).isEqualTo("AWS::EC2::Instance");
  }

  @Test
  public void getMaxCacheSize_no_env_default_return() throws Exception {
    long ret = UDPMessageEncoder.getMaxCacheSize();
    assertThat(ret).isEqualTo(1000);
  }

  @Test
  public void getMaxCacheSize_env_value_return() throws Exception {
    updateEnv("AWS_XRAY_CACHE_SIZE", "2500");
    long ret = UDPMessageEncoder.getMaxCacheSize();
    assertThat(ret).isEqualTo(2500);
  }

  @Test
  public void getTtlInSeconds_no_env_default_return() throws Exception {
    long ret = UDPMessageEncoder.getTtlInSeconds();
    assertThat(ret).isEqualTo(3600);
  }

  @Test
  public void getTtlInSeconds_env_value_return() throws Exception {
    updateEnv("AWS_XRAY_CACHE_TTL_SECONDS", "60");
    long ret = UDPMessageEncoder.getTtlInSeconds();
    assertThat(ret).isEqualTo(60);
  }

  @SuppressWarnings({"unchecked"})
  public static void updateEnv(String name, String val) throws ReflectiveOperationException {
    Map<String, String> env = System.getenv();
    Field field = env.getClass().getDeclaredField("m");
    field.setAccessible(true);
    ((Map<String, String>) field.get(env)).put(name, val);
  }

  @SuppressWarnings({"unchecked"})
  public static void removeEnv(String name) throws ReflectiveOperationException {
    Map<String, String> env = System.getenv();
    Field field = env.getClass().getDeclaredField("m");
    field.setAccessible(true);
    ((Map<String, String>) field.get(env)).remove(name);
  }

  @SuppressWarnings({"unchecked"})
  public static void removeEnvAll() throws ReflectiveOperationException {
    Map<String, String> env = System.getenv();
    Field field = env.getClass().getDeclaredField("m");
    field.setAccessible(true);
    ((Map<String, String>) field.get(env)).clear();
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
