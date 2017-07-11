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
package zipkin.collector.sqs;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.collector.CollectorComponent;
import zipkin.collector.CollectorSampler;
import zipkin.collector.InMemoryCollectorMetrics;
import zipkin.junit.aws.AmazonSQSRule;
import zipkin.storage.InMemoryStorage;
import zipkin.storage.QueryRequest;
import zipkin.storage.StorageComponent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static zipkin.TestObjects.TRACE;

public class SQSCollectorTest {

  @Rule
  public AmazonSQSRule sqsRule = new AmazonSQSRule().start(9324);

  private StorageComponent store;

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

  @Test
  public void collectSpans() throws Exception {

    sqsRule.sendSpans(TRACE);

    await()
        .atMost(15, TimeUnit.SECONDS)
        .until(() -> store.spanStore().getRawTrace(TRACE.get(0).traceId) != null);

    List<Span> fromStore = store.spanStore().getRawTrace(TRACE.get(0).traceId);

    assertThat(metrics.messagesDropped()).as("check dropped metrics.").isEqualTo(0);
    assertThat(fromStore).as("recorded spans should not be null").isNotNull();
    assertThat(fromStore.size()).as("all spans have been recorded").isEqualTo(TRACE.size());
    assertThat(sqsRule.queueCount()).as("accepted spans are deleted.").isEqualTo(0);
  }

  @Test
  public void collectLotsOfSpans() throws Exception {

    Span[] LOTS = new Random().longs(10000L).mapToObj(TestObjects::span).toArray(Span[]::new);

    sqsRule.sendSpans(Arrays.asList(LOTS));

    QueryRequest query = QueryRequest.builder().serviceName("service").limit(LOTS.length).build();

    await()
        .atMost(60, TimeUnit.SECONDS)
        .until(() -> {
          List<List<Span>> spans = store.spanStore().getTraces(query);
          return (spans != null && spans.size() == LOTS.length);
        });

    List<List<Span>> fromStore = store.spanStore().getTraces(query);

    assertThat(metrics.messagesDropped()).as("check dropped metrics.").isEqualTo(0);
    assertThat(fromStore).as("recorded spans should not be null").isNotNull();
    assertThat(fromStore.size()).as("all spans have been accepted.").isEqualTo(LOTS.length);
    assertThat(sqsRule.queueCount()).as("accepted spans as deleted.").isEqualTo(0);
  }

}
