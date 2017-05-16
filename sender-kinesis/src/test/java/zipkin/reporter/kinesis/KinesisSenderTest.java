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
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.EnhancedMetrics;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
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
  public void checkPasses() throws InterruptedException, JsonProcessingException {
    StreamDescription streamDescription = new StreamDescription()
        .withStreamStatus(StreamStatus.ACTIVE)
        .withEnhancedMonitoring(new EnhancedMetrics()
            .withShardLevelMetrics("ALL")
        )
        .withHasMoreShards(false)
        .withRetentionPeriodHours(24)
        .withShards(new Shard()
            .withHashKeyRange(new HashKeyRange().withStartingHashKey("0").withEndingHashKey("0"))
            .withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("0"))
            .withShardId("shard")
        )
        .withStreamARN("arn:aws:kinesis:us-east-1:111122223333:test")
        .withStreamCreationTimestamp(new Date())
        .withStreamName("test");

    DescribeStreamResult describeStreamResult = new DescribeStreamResult();
    describeStreamResult.setStreamDescription(streamDescription);
    server.enqueue(new MockResponse()
        .addHeader("Content-Type", "application/x-amz-cbor-1.1")
        .addHeader("x-amzn-RequestId", "1234")
        .setBody(new Buffer().write(mapper.writeValueAsBytes(describeStreamResult))));

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
