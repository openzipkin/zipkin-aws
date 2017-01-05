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
package zipkin.server;

import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import zipkin.collector.CollectorComponent;
import zipkin.collector.sqs.SQSCollector;
import zipkin.junit.aws.AmazonSQSRule;
import zipkin.storage.SpanStore;
import zipkin.storage.StorageComponent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static zipkin.TestObjects.TRACE;

@SpringBootTest(classes = ZipkinServer.class, properties = {
    "zipkin.storage.type=mem",
    "spring.config.name=zipkin-server",
    "zipkin.collector.scribe.enabled=false",
    "zipkin.collector.sqs.queue-url=http://localhost:9324/queue/zipkin",
    "zipkin.collector.sqs.aws-access-key-id=x",
    "zipkin.collector.sqs.aws-secret-access-key=x"
})
@RunWith(SpringRunner.class)
public class ZipkinServerSQSCollectorTest {

  @Rule
  public AmazonSQSRule sqsRule = new AmazonSQSRule().start(9324);

  @Autowired
  ConfigurableWebApplicationContext context;

  @Test
  public void sqsCollectorComponent() throws Exception {
    assertThat(context.getBean(CollectorComponent.class)).isInstanceOf(SQSCollector.class);
  }

  @Test
  public void sqsSpansReceived() throws Exception {
    sqsRule.sendSpans(TRACE);

    System.out.print(sqsRule.queueUrl());

    SpanStore store = context.getBean(StorageComponent.class).spanStore();

    await()
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> store.getRawTrace(TRACE.get(0).traceId) != null);

    assertThat(store.getRawTrace(TRACE.get(0).traceId).size()).isEqualTo(TRACE.size());
  }

}
