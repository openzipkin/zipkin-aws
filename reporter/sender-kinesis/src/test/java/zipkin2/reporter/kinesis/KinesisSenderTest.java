/*
 * Copyright 2016-2024 The OpenZipkin Authors
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
package zipkin2.reporter.kinesis;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.SocketPolicy;
import okio.Buffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.SpanBytesEncoder;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;

class KinesisSenderTest {
  public MockWebServer server = new MockWebServer();

  // Kinesis sends data in CBOR format
  ObjectMapper mapper = new ObjectMapper(new CBORFactory());
  KinesisSender sender;

  @BeforeEach void setup() {
    sender =
        KinesisSender.newBuilder()
            .streamName("test")
            .endpointConfiguration(
                new EndpointConfiguration(server.url("/").toString(), "us-east-1"))
            .credentialsProvider(
                new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
  }

  @Test void send() throws Exception {
    server.enqueue(new MockResponse());

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);

    assertThat(extractSpans(server.takeRequest().getBody()))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void send_empty() throws Exception {
    server.enqueue(new MockResponse());

    sendSpans();

    assertThat(extractSpans(server.takeRequest().getBody()))
        .isEmpty();
  }

  @Test void send_PROTO3() throws Exception {
    server.enqueue(new MockResponse());

    sender.close();
    sender = sender.toBuilder().encoding(Encoding.PROTO3).build();

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);

    assertThat(extractSpans(server.takeRequest().getBody()))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void send_json_unicode() throws Exception {
    server.enqueue(new MockResponse());

    Span unicode = CLIENT_SPAN.toBuilder().putTag("error", "\uD83D\uDCA9").build();
    sendSpans(unicode);

    assertThat(extractSpans(server.takeRequest().getBody())).containsExactly(unicode);
  }

  @Test void sendFailsWithException() {
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));
    // 3 retries after initial failure
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));

    assertThatThrownBy(this::sendSpans)
        .isInstanceOf(SdkClientException.class);
  }

  List<Span> extractSpans(Buffer body) throws IOException {
    byte[] encodedSpans = mapper.readTree(body.inputStream()).get("Data").binaryValue();
    if (encodedSpans[0] == '[') {
      return SpanBytesDecoder.JSON_V2.decodeList(encodedSpans);
    }
    return SpanBytesDecoder.PROTO3.decodeList(encodedSpans);
  }

  void sendSpans(zipkin2.Span... spans) {
    SpanBytesEncoder bytesEncoder =
        sender.encoding() == Encoding.JSON ? SpanBytesEncoder.JSON_V2 : SpanBytesEncoder.PROTO3;
    sender.send(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

  @AfterEach void afterEachTest() throws IOException {
    server.close();
  }
}
