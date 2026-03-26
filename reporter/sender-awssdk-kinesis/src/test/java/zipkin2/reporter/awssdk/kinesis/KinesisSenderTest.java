/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.awssdk.kinesis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.SocketPolicy;
import okio.Buffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
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

  // V2 SDK sync client sends JSON wire format
  ObjectMapper mapper = new ObjectMapper();
  KinesisSender sender;

  @BeforeEach void setup() {
    // Disable CBOR so the SDK sends JSON that MockWebServer can handle
    System.setProperty("software.amazon.awssdk.http.async.service.impl", "");
    System.setProperty("aws.cborEnabled", "false");
    KinesisClient kinesisClient = KinesisClient.builder()
        .httpClient(UrlConnectionHttpClient.create())
        .endpointOverride(URI.create(server.url("/").toString()))
        .region(Region.US_EAST_1)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
        .build();

    sender = KinesisSender.newBuilder()
        .streamName("test")
        .kinesisClient(kinesisClient)
        .build();
  }

  @Test void send() throws Exception {
    server.enqueue(kinesisResponse());

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);

    assertThat(extractSpans(server.takeRequest().getBody()))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void send_empty() throws Exception {
    server.enqueue(kinesisResponse());

    sendSpans();

    assertThat(extractSpans(server.takeRequest().getBody()))
        .isEmpty();
  }

  @Test void send_PROTO3() throws Exception {
    server.enqueue(kinesisResponse());

    sender.close();
    sender = sender.toBuilder().encoding(Encoding.PROTO3).build();

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);

    assertThat(extractSpans(server.takeRequest().getBody()))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void send_json_unicode() throws Exception {
    server.enqueue(kinesisResponse());

    Span unicode = CLIENT_SPAN.toBuilder().putTag("error", "\uD83D\uDCA9").build();
    sendSpans(unicode);

    assertThat(extractSpans(server.takeRequest().getBody())).containsExactly(unicode);
  }

  @Test void sendFailsWithException() {
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));

    assertThatThrownBy(this::sendSpans)
        .isInstanceOf(Exception.class);
  }

  static MockResponse kinesisResponse() {
    return new MockResponse()
        .addHeader("Content-Type", "application/x-amz-json-1.1")
        .setBody("{\"SequenceNumber\":\"1\",\"ShardId\":\"shardId-000000000000\"}");
  }

  List<Span> extractSpans(Buffer body) throws IOException {
    JsonNode tree = mapper.readTree(body.inputStream());
    // V2 SDK sends JSON with "Data" as base64-encoded bytes
    byte[] encodedSpans = Base64.getDecoder().decode(tree.get("Data").asText());
    if (encodedSpans[0] == '[') {
      return SpanBytesDecoder.JSON_V2.decodeList(encodedSpans);
    }
    return SpanBytesDecoder.PROTO3.decodeList(encodedSpans);
  }

  void sendSpans(Span... spans) {
    SpanBytesEncoder bytesEncoder =
        sender.encoding() == Encoding.JSON ? SpanBytesEncoder.JSON_V2 : SpanBytesEncoder.PROTO3;
    sender.send(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

  @AfterEach void afterEachTest() throws IOException {
    sender.close();
    server.close();
  }
}
