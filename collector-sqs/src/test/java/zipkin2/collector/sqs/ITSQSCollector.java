/*
 * Copyright 2016-2019 The OpenZipkin Authors
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
package zipkin2.collector.sqs;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import zipkin2.Span;
import zipkin2.TestObjects;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.collector.CollectorComponent;
import zipkin2.collector.CollectorSampler;
import zipkin2.collector.InMemoryCollectorMetrics;
import zipkin2.junit.aws.AmazonSQSRule;
import zipkin2.storage.InMemoryStorage;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ITSQSCollector {

  @Rule public AmazonSQSRule sqsRule = new AmazonSQSRule().start(9324);

  List<Span> spans =
      asList( // No unicode or data that doesn't translate between json formats
          TestObjects.LOTS_OF_SPANS[0], TestObjects.LOTS_OF_SPANS[1], TestObjects.LOTS_OF_SPANS[2]);

  InMemoryStorage store;
  InMemoryCollectorMetrics metrics;
  CollectorComponent collector;

  @Before public void setup() {
    store = InMemoryStorage.newBuilder().build();
    metrics = new InMemoryCollectorMetrics();

    collector = new SQSCollector.Builder()
        .queueUrl(sqsRule.queueUrl())
        .parallelism(2)
        .waitTimeSeconds(1) // using short wait time to make test teardown faster
        .endpointOverride(URI.create(sqsRule.queueUrl()))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
        .metrics(metrics)
        .sampler(CollectorSampler.ALWAYS_SAMPLE)
        .storage(store)
        .build()
        .start();
    metrics = metrics.forTransport("sqs");
  }

  @After public void teardown() throws IOException {
    store.close();
    collector.close();
  }

  /** SQS has character constraints on json, so some traces will be base64 even if json */
  @Test public void collectBase64EncodedSpans() throws Exception {
    sqsRule.send(Base64.getEncoder().encodeToString(SpanBytesEncoder.JSON_V1.encodeList(spans)));
    assertSpansAccepted(spans);
  }

  /** SQS has character constraints on json, but don't affect traces not using unicode */
  @Test public void collectUnencodedJsonSpans() throws Exception {
    sqsRule.send(new String(SpanBytesEncoder.JSON_V1.encodeList(spans), "UTF-8"));
    assertSpansAccepted(spans);
  }

  /** Ensures list encoding works: a version 2 json list of spans */
  @Test public void messageWithMultipleSpans_json2() throws Exception {
    messageWithMultipleSpans(SpanBytesEncoder.JSON_V2);
  }

  /** Ensures list encoding works: proto3 ListOfSpans */
  @Test public void messageWithMultipleSpans_proto3() throws Exception {
    messageWithMultipleSpans(SpanBytesEncoder.PROTO3);
  }

  void messageWithMultipleSpans(SpanBytesEncoder encoder) throws Exception {
    byte[] message = encoder.encodeList(spans);
    sqsRule.send(Base64.getEncoder().encodeToString(message));
    assertSpansAccepted(spans);
  }

  @Test public void collectLotsOfSpans() throws Exception {
    List<Span> lots = new ArrayList<>(10000);

    int count = 0;
    List<Span> bucket = new ArrayList<>();

    for (int i = 0; i < 10000; i++) {
      Span span = TestObjects.span(i + 1);
      lots.add(span);
      bucket.add(span);
      if (count++ > 9) {
        sqsRule.send(new String(SpanBytesEncoder.JSON_V1.encodeList(bucket), "UTF-8"));
        bucket = new ArrayList<>();
        count = 0;
      }
    }
    sqsRule.send(new String(SpanBytesEncoder.JSON_V1.encodeList(bucket), "UTF-8"));

    assertSpansAccepted(lots);
  }

  @Test public void malformedSpansShouldBeDiscarded() throws Exception {
    sqsRule.send("[not going to work]");
    sqsRule.send(new String(SpanBytesEncoder.JSON_V1.encodeList(spans)));

    await().atMost(15, TimeUnit.SECONDS).until(() -> store.acceptedSpanCount() == spans.size());

    assertThat(metrics.messages()).isEqualTo(2);
    assertThat(metrics.messagesDropped()).isEqualTo(1); // only one failed
    assertThat(metrics.bytes()).isPositive();

    // ensure corrupt spans are deleted
    await().atMost(5, TimeUnit.SECONDS).until(() -> sqsRule.notVisibleCount() == 0);
  }

  void assertSpansAccepted(List<Span> spans) throws Exception {
    await().atMost(15, TimeUnit.SECONDS).until(() -> store.acceptedSpanCount() == spans.size());

    List<Span> someSpans = store.spanStore().getTrace(spans.get(0).traceId()).execute();

    assertThat(metrics.messages()).as("check accept metrics.").isPositive();
    assertThat(metrics.bytes()).as("check bytes metrics.").isPositive();
    assertThat(metrics.messagesDropped()).as("check dropped metrics.").isEqualTo(0);
    assertThat(someSpans).as("recorded spans should not be null").isNotNull();
    assertThat(spans).as("some spans have been recorded").containsAll(someSpans);
    assertThat(sqsRule.queueCount()).as("accepted spans are deleted.").isEqualTo(0);
  }
}
