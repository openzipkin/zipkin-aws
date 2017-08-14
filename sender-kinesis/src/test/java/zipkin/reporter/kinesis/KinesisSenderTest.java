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
package zipkin.reporter.kinesis;

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
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import okio.Buffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import zipkin.Codec;
import zipkin.Component;
import zipkin.Span;
import zipkin.SpanDecoder;
import zipkin.TestObjects;
import zipkin.internal.ApplyTimestampAndDuration;
import zipkin.internal.Span2Codec;
import zipkin.internal.Span2Converter;
import zipkin.internal.Util;
import zipkin.reporter.Encoder;
import zipkin.reporter.Encoding;
import zipkin.reporter.internal.AwaitableCallback;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class KinesisSenderTest {
  @Rule public MockWebServer server = new MockWebServer();

  List<Span> spans = asList( // No unicode or data that doesn't translate between json formats 
      ApplyTimestampAndDuration.apply(TestObjects.LOTS_OF_SPANS[0]),
      ApplyTimestampAndDuration.apply(TestObjects.LOTS_OF_SPANS[1]),
      ApplyTimestampAndDuration.apply(TestObjects.LOTS_OF_SPANS[2])
  );

  // Kinesis sends data in CBOR format
  ObjectMapper mapper = new ObjectMapper(new CBORFactory());
  KinesisSender sender;

  @Before
  public void setup() throws Exception {
    sender = KinesisSender.builder()
        .streamName("test")
        .endpointConfiguration(new EndpointConfiguration(server.url("/").toString(), "us-east-1"))
        .credentialsProvider(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
        .build();
  }

  @Test
  public void sendsSpans_thrift() throws Exception {
    sendsSpans(Encoder.THRIFT, spans);
  }

  /** Kinesis is strict with regards to all data being Base64 encoded, even ascii data */
  @Test
  public void base64EncodesAsciiJson() throws Exception {
    server.enqueue(new MockResponse());
    sender = sender.toBuilder().encoding(Encoding.JSON).build();
    send(Encoder.JSON, spans);

    RecordedRequest request = server.takeRequest();
    byte[] encodedSpans = // binaryValue base64 decodes
        mapper.readTree(request.getBody().inputStream()).get("Data").binaryValue();

    assertThat(new String(encodedSpans))
        .isEqualTo(new String(Codec.JSON.writeSpans(spans)));
  }

  @Test
  public void sendsSpans_json() throws Exception {
    sender.close();
    sender = sender.toBuilder().encoding(Encoding.JSON).build();
    sendsSpans(Encoder.JSON, spans);
  }

  @Test
  public void sendsSpans_json2() throws Exception {
    sender.close();
    sender = sender.toBuilder().encoding(Encoding.JSON).build();

    // temporary span2 encoder until the type is made public
    sendsSpans(new Encoder<Span>() {
      @Override public Encoding encoding() {
        return Encoding.JSON;
      }

      @Override public byte[] encode(Span span) {
        return Span2Codec.JSON.writeSpan(Span2Converter.fromSpan(span).get(0));
      }
    }, spans);
  }

  <S> void sendsSpans(Encoder<S> encoder, List<S> spans) throws Exception {
    server.enqueue(new MockResponse());
    send(encoder, spans);

    RecordedRequest request = server.takeRequest();
    assertThat(extractSpans(request.getBody()))
        .isEqualTo(spans);
  }

  @Test
  public void checkPasses() throws Exception {
    enqueueCborResponse(mapper.createObjectNode().set("StreamDescription",
        mapper.createObjectNode().put("StreamStatus", "ACTIVE")));

    Component.CheckResult result = sender.check();
    assertThat(result.ok).isTrue();
  }

  @Test
  public void checkFailsWithStreamNotActive() throws Exception {
    enqueueCborResponse(mapper.createObjectNode().set("StreamDescription",
        mapper.createObjectNode().put("StreamStatus", "DELETING")));

    Component.CheckResult result = sender.check();
    assertThat(result.ok).isFalse();
    assertThat(result.exception).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void checkFailsWithException() {
    server.enqueue(new MockResponse()
        .setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));
    // 3 retries after initial failure
    server.enqueue(new MockResponse()
        .setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));
    server.enqueue(new MockResponse()
        .setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));
    server.enqueue(new MockResponse()
        .setSocketPolicy(SocketPolicy.DISCONNECT_DURING_REQUEST_BODY));

    Component.CheckResult result = sender.check();
    assertThat(result.ok).isFalse();
    assertThat(result.exception).isInstanceOf(SdkClientException.class);
  }

  void enqueueCborResponse(JsonNode document) throws JsonProcessingException {
    server.enqueue(new MockResponse()
        .addHeader("Content-Type", "application/x-amz-cbor-1.1")
        .setBody(new Buffer().write(mapper.writeValueAsBytes(document))));
  }

  List<Span> extractSpans(Buffer body) throws IOException {
    byte[] encodedSpans = mapper.readTree(body.inputStream()).get("Data").binaryValue();
    return SpanDecoder.DETECTING_DECODER.readSpans(encodedSpans);
  }

  <S> void send(Encoder<S> encoder, List<S> spans) {
    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(spans.stream().map(encoder::encode).collect(toList()), callback);
    callback.await();
  }
}
