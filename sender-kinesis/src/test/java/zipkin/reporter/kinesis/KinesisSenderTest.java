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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import java.io.IOException;
import java.util.List;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import zipkin.Codec;
import zipkin.Component;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.reporter.Encoder;
import zipkin.reporter.internal.AwaitableCallback;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class KinesisSenderTest {
  @Rule public MockWebServer server = new MockWebServer();

  // Kinesis sends data in CBOR format
  ObjectMapper mapper = new ObjectMapper(new CBORFactory());
  KinesisSender sender;

  @Before
  public void setup() throws IOException {
    sender = KinesisSender.builder()
        .streamName("test")
        .endpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(server.url("/").toString(), "us-east-1"))
        .credentialsProvider(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
        .build();
  }

  @Test
  public void sendsSpans() throws Exception {
    server.enqueue(new MockResponse());

    send(TestObjects.TRACE);

    RecordedRequest request = server.takeRequest();
    assertThat(extractSpans(request.getBody()))
        .isEqualTo(TestObjects.TRACE);
  }

  @Test
  public void checkPasses() throws InterruptedException {
    server.enqueue(new MockResponse()
        .addHeader("Content-Type", "application/x-amz-json-1.1")
        .addHeader("x-amzn-RequestId", "1234")
        .setBody("{\"StreamDescription\":{\"EnhancedMonitoring\":[{\"ShardLevelMetrics\":[]}],\"HasMoreShards\":false,\"RetentionPeriodHours\":24,\"Shards\":[{\"HashKeyRange\":{\"EndingHashKey\":\"340282366920938463463374607431768211455\",\"StartingHashKey\":\"0\"},\"SequenceNumberRange\":{\"StartingSequenceNumber\":\"49573122618435842026682462638596372868559861341178822658\"},\"ShardId\":\"shardId-000000000000\"}],\"StreamARN\":\"arn:aws:kinesis:us-east-1:1122334455:stream/test\",\"StreamCreationTimestamp\":1.494527725E9,\"StreamName\":\"test\",\"StreamStatus\":\"ACTIVE\"}}"));
    server.enqueue(new MockResponse());

    Component.CheckResult result = sender.check();
    server.takeRequest();
    assertThat(result.ok).isTrue();
  }

  //@Test
  //public void checkFailsWithStreamNotActive() throws IOException {
  //  server.enqueue(new MockResponse()
  //      .addHeader("Content-Type", "application/x-amz-json-1.1")
  //      .setBody("{\"StreamDescription\": {\"StreamStatus\": \"DELETING\"}}"));
  //
  //  Component.CheckResult result = sender.check();
  //  assertThat(result.ok).isFalse();
  //  assertThat(result.exception).isInstanceOf(IllegalStateException.class);
  //}

  @Test
  public void checkFailsWithException() {
    server.enqueue(new MockResponse());

    Component.CheckResult result = sender.check();
    assertThat(result.ok).isFalse();
    assertThat(result.exception).isInstanceOf(NullPointerException.class);
  }

  List<Span> extractSpans(Buffer body) throws IOException {
    byte[] thriftEncodedSpans = mapper.readTree(body.inputStream()).get("Data").binaryValue();
    return Codec.THRIFT.readSpans(thriftEncodedSpans);
  }

  void send(List<Span> spans) {
    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(spans.stream().map(Encoder.THRIFT::encode).collect(toList()), callback);
    callback.await();
  }
}
