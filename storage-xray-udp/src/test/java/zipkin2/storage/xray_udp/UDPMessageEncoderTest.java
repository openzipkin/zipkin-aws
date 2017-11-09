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

import java.io.IOException;
import org.junit.Test;
import zipkin2.Endpoint;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

public class UDPMessageEncoderTest {

  @Test
  public void doEncodeServer() throws Exception {
    String target = "{\"format\": \"json\", \"version\": 1}\n"
                    + "{\"trace_id\":\"1-12345678-90abcdef1234567890abcdef\",\"id\":\"1234567890abcdef\"}";
    Span span = Span
        .newBuilder()
        .kind(Span.Kind.SERVER)
        .name("test-cemo")
        .id("1234567890abcdef")
        .traceId("1234567890abcdef1234567890abcdef")
        .shared(false)
        .build();

    String spanString = asString(span);

    assertThat(target).isEqualTo(spanString);
  }

  @Test
  public void doEncodeClient() throws Exception {
    String target = "{\"format\": \"json\", \"version\": 1}\n"
                    + "{\"trace_id\":\"1-12345678-90abcdef1234567890abcdef\",\"id\":\"1234567890abcdef\","
                    + "\"type\":\"subsegment\",\"namespace\":\"remote\",\"name\":\"master\"}";
    Span span = Span
        .newBuilder()
        .kind(Span.Kind.CLIENT)
        .remoteEndpoint(Endpoint.newBuilder().serviceName("master").build())
        .name("test-cemo")
        .id("1234567890abcdef")
        .traceId("1234567890abcdef1234567890abcdef")
        .shared(false)
        .build();

    String spanString = asString(span);

    assertThat(target).isEqualTo(spanString);
  }

  @Test
  public void doEncodeSql() throws Exception {
    String target = "{\"format\": \"json\", \"version\": 1}\n"
                    + "{\"trace_id\":\"1-12345678-90abcdef1234567890abcdef\",\"id\":\"1234567890abcdef\","
                    + "\"type\":\"subsegment\",\"namespace\":\"remote\",\"name\":\"master\","
                    + "\"sql\":{\"url\":\"jdbc:test\"}}";
    Span span = Span
        .newBuilder()
        .kind(Span.Kind.CLIENT)
        .remoteEndpoint(Endpoint.newBuilder().serviceName("master").build())
        .name("test-cemo")
        .id("1234567890abcdef")
        .traceId("1234567890abcdef1234567890abcdef")
        .putTag("sql.url", "jdbc:test")
        .shared(false)
        .build();

    String spanString = asString(span);

    assertThat(target).isEqualTo(spanString);
  }

  @Test
  public void doEncodeUnkown() throws Exception {
    String target = "{\"format\": \"json\", \"version\": 1}\n"
                    + "{\"trace_id\":\"1-12345678-90abcdef1234567890abcdef\",\"id\":\"1234567890abcdef\","
                    + "\"type\":\"subsegment\",\"namespace\":\"remote\",\"name\":\"unknown\","
                    + "\"sql\":{\"url\":\"jdbc:test\"}}";
    Span span = Span
        .newBuilder()
        .kind(Span.Kind.CLIENT)
        .name("test-cemo")
        .remoteEndpoint(Endpoint.newBuilder().build())
        .id("1234567890abcdef")
        .traceId("1234567890abcdef1234567890abcdef")
        .putTag("sql.url", "jdbc:test")
        .shared(false)
        .build();

    String spanString = asString(span);

    assertThat(target).isEqualTo(spanString);
  }

  @Test
  public void doEncodeAws() throws Exception {
    String target = "{\"format\": \"json\", \"version\": 1}\n"
                    + "{\"trace_id\":\"1-12345678-90abcdef1234567890abcdef\",\"id\":\"1234567890abcdef\","
                    + "\"type\":\"subsegment\",\"namespace\":\"remote\",\"name\":\"unknown\","
                    + "\"aws\":{\"region\":\"reg1\",\"table_name\":\"table1\"}}";
    Span span = Span
        .newBuilder()
        .kind(Span.Kind.CLIENT)
        .name("test-cemo")
        .remoteEndpoint(Endpoint.newBuilder().build())
        .id("1234567890abcdef")
        .traceId("1234567890abcdef1234567890abcdef")
        .putTag("aws.region", "reg1")
        .putTag("aws.table_name", "table1")
        .shared(false)
        .build();

    String spanString = asString(span);

    assertThat(target).isEqualTo(spanString);
  }

  protected String asString(Span span) throws IOException {
    return new String(UDPMessageEncoder.doEncode(span));
  }
}