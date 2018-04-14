/**
 * Copyright 2016-2018 The OpenZipkin Authors
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
package zipkin.collector.sqs;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import zipkin.Codec;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.collector.CollectorComponent;
import zipkin.collector.CollectorSampler;
import zipkin.collector.InMemoryCollectorMetrics;
import zipkin.internal.ApplyTimestampAndDuration;
import zipkin.internal.Util;
import zipkin.junit.aws.AmazonSQSRule;
import zipkin.storage.InMemoryStorage;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class SQSCollectorTest {

  @Rule
  public AmazonSQSRule sqsRule = new AmazonSQSRule().start(9324);

  List<Span> spans = asList( // No unicode or data that doesn't translate between json formats
      ApplyTimestampAndDuration.apply(TestObjects.LOTS_OF_SPANS[0]),
      ApplyTimestampAndDuration.apply(TestObjects.LOTS_OF_SPANS[1]),
      ApplyTimestampAndDuration.apply(TestObjects.LOTS_OF_SPANS[2])
  );

  private InMemoryStorage store;

  private InMemoryCollectorMetrics metrics;

  private CollectorComponent collector;

  @Before
  public void setup() {
    store = new InMemoryStorage();
    metrics = new InMemoryCollectorMetrics();

    collector = new SQSCollector.Builder()
        .queueUrl(sqsRule.queueUrl())
        .parallelism(2)
        .waitTimeSeconds(1) // using short wait time to make test teardown faster
        .endpointConfiguration(new EndpointConfiguration(sqsRule.queueUrl(), "us-east-1"))
        .credentialsProvider(new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x")))
        .metrics(metrics)
        .sampler(CollectorSampler.ALWAYS_SAMPLE)
        .storage(store)
        .build()
        .start();
  }

  @After
  public void teardown() throws Exception {
    store.close();
    collector.close();
  }

  /** SQS has character constraints on json, so some traces will be base64 even if json */
  @Test
  public void collectBase64EncodedSpans() throws Exception {
    sqsRule.send(Base64.getEncoder().encodeToString(Codec.JSON.writeSpans(spans)));
    assertSpansAccepted(spans);
  }

  /** SQS has character constraints on json, but don't affect traces not using unicode */
  @Test
  public void collectUnencodedJsonSpans() throws Exception {
    sqsRule.send(new String(Codec.JSON.writeSpans(spans), Util.UTF_8));
    assertSpansAccepted(spans);
  }

  @Test
  public void collectLotsOfSpans() throws Exception {
    List<Span> lots = new ArrayList<>(10000);

    int count = 0;
    List<Span> bucket = new ArrayList<>();

    for (int i = 0; i < 10000; i++) {
      Span span = TestObjects.span(i + 1);
      lots.add(span);
      bucket.add(span);
      if (count++ > 9) {
        sqsRule.send(new String(Codec.JSON.writeSpans(bucket), Util.UTF_8));
        bucket = new ArrayList<>();
        count = 0;
      }
    }
    sqsRule.send(new String(Codec.JSON.writeSpans(bucket), Util.UTF_8));

    assertSpansAccepted(lots);
  }

  @Test
  public void malformedSpansShouldBeDiscarded() throws Exception {
    sqsRule.send("[not going to work]");
    sqsRule.send(new String(Codec.JSON.writeSpans(spans)));
    assertSpansAccepted(spans);

    assertThat(sqsRule.notVisibleCount()).as("corrupt spans are deleted.").isEqualTo(0);
  }

  void assertSpansAccepted(List<Span> spans) {
    await().atMost(15, TimeUnit.SECONDS).until(() -> store.acceptedSpanCount() == spans.size());

    List<Span> someSpans =
        store.spanStore().getRawTrace(spans.get(0).traceIdHigh, spans.get(0).traceId);

    assertThat(metrics.messagesDropped()).as("check dropped metrics.").isEqualTo(0);
    assertThat(someSpans).as("recorded spans should not be null").isNotNull();
    assertThat(spans).as("some spans have been recorded").containsAll(someSpans);
    assertThat(sqsRule.queueCount()).as("accepted spans are deleted.").isEqualTo(0);
  }
}
