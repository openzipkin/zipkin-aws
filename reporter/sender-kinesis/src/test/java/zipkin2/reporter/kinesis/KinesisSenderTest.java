/*
 * Copyright 2016-2023 The OpenZipkin Authors
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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.SocketPolicy;
import okio.Buffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.CheckResult;
import zipkin2.Span;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
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
            .credentialsProvider(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();
  }

  @Test void sendsSpans() throws Exception {
    server.enqueue(new MockResponse());

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(extractSpans(server.takeRequest().getBody()))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void sendsSpans_PROTO3() throws Exception {
    server.enqueue(new MockResponse());

    sender.close();
    sender = sender.toBuilder().encoding(Encoding.PROTO3).build();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(extractSpans(server.takeRequest().getBody()))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void outOfBandCancel() throws Exception {
    server.enqueue(new MockResponse());

    KinesisSender.KinesisCall call = (KinesisSender.KinesisCall) send(CLIENT_SPAN, CLIENT_SPAN);
    assertThat(call.isCanceled()).isFalse(); // sanity check

    CountDownLatch latch = new CountDownLatch(1);
    call.enqueue(
        new Callback<Void>() {
          @Override
          public void onSuccess(Void aVoid) {
            call.future.cancel(true);
            latch.countDown();
          }

          @Override
          public void onError(Throwable throwable) {
            latch.countDown();
          }
        });

    latch.await(5, TimeUnit.SECONDS);
    assertThat(call.isCanceled()).isTrue();
  }

  @Test void sendsSpans_json_unicode() throws Exception {
    server.enqueue(new MockResponse());

    Span unicode = CLIENT_SPAN.toBuilder().putTag("error", "\uD83D\uDCA9").build();
    send(unicode).execute();

    assertThat(extractSpans(server.takeRequest().getBody())).containsExactly(unicode);
  }

  @Test void checkPasses() throws Exception {
    enqueueCborResponse(
        mapper
            .createObjectNode()
            .set("StreamDescription", mapper.createObjectNode().put("StreamStatus", "ACTIVE")));

    CheckResult result = sender.check();
    assertThat(result.ok()).isTrue();
  }

  @Test void checkFailsWithStreamNotActive() throws Exception {
    enqueueCborResponse(
        mapper
            .createObjectNode()
            .set("StreamDescription", mapper.createObjectNode().put("StreamStatus", "DELETING")));

    CheckResult result = sender.check();
    assertThat(result.ok()).isFalse();
    assertThat(result.error()).isInstanceOf(IllegalStateException.class);
  }

  @Test void checkFailsWithException() {
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));
    // 3 retries after initial failure
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));

    CheckResult result = sender.check();
    assertThat(result.ok()).isFalse();
    assertThat(result.error()).isInstanceOf(SdkClientException.class);
  }

  void enqueueCborResponse(JsonNode document) throws JsonProcessingException {
    server.enqueue(
        new MockResponse()
            .addHeader("Content-Type", "application/x-amz-cbor-1.1")
            .setBody(new Buffer().write(mapper.writeValueAsBytes(document))));
  }

  List<Span> extractSpans(Buffer body) throws IOException {
    byte[] encodedSpans = mapper.readTree(body.inputStream()).get("Data").binaryValue();
    if (encodedSpans[0] == '[') {
      return SpanBytesDecoder.JSON_V2.decodeList(encodedSpans);
    }
    return SpanBytesDecoder.PROTO3.decodeList(encodedSpans);
  }

  Call<Void> send(zipkin2.Span... spans) {
    SpanBytesEncoder bytesEncoder =
        sender.encoding() == Encoding.JSON ? SpanBytesEncoder.JSON_V2 : SpanBytesEncoder.PROTO3;
    return sender.sendSpans(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

  @AfterEach void afterEachTest() throws IOException {
    server.close();
  }
}
