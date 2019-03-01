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
package brave.instrumentation.aws.sqs;

import brave.Tracing;
import brave.context.log4j2.ThreadContextScopeDecorator;
import brave.propagation.StrictScopeDecorator;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.sampler.Sampler;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

public class SendMessageTracingRequestHandlerTest {
  private BlockingQueue<Span> spans = new LinkedBlockingQueue<>();

  Tracing tracing;
  TraceContext.Extractor<Map<String, MessageAttributeValue>> extractor;
  SendMessageTracingRequestHandler handler;

  @Before
  public void setup() {
    tracing = tracingBuilder().build();
    extractor = tracing.propagation().extractor(SendMessageTracingRequestHandler.GETTER);

    handler = new SendMessageTracingRequestHandler(tracing);
  }

  @After
  public void cleanup() {
    tracing.close();
  }

  @Test
  public void handleSendMessageRequest() throws InterruptedException {
    SendMessageRequest request = new SendMessageRequest("queueUrl", "test message content");

    handler.beforeExecution(request);

    // Verify propagation
    verifyInjectedTraceContext(request.getMessageAttributes(), null);

    // Verify Span
    Span reportedSpan = spans.take();
    verifyReportedPublishSpan(reportedSpan);

    assertThat(spans.size()).isEqualTo(0);
  }

  @Test
  public void handleSendMessageBatchRequest() throws InterruptedException {
    SendMessageBatchRequest request = new SendMessageBatchRequest("queueUrl");
    SendMessageBatchRequestEntry entry1 =
        new SendMessageBatchRequestEntry("id1", "test message body 1");
    SendMessageBatchRequestEntry entry2 =
        new SendMessageBatchRequestEntry("id2", "test message body 2");
    request.withEntries(entry1, entry2);

    handler.beforeExecution(request);

    List<Span> reportedSpans = Arrays.asList(spans.take(), spans.take(), spans.take());

    Span localParent = reportedSpans.stream().filter(s -> s.parentId() == null).findFirst().get();

    // Verify propagation
    for (SendMessageBatchRequestEntry entry : request.getEntries()) {
      verifyInjectedTraceContext(entry.getMessageAttributes(), localParent);
    }

    // Verify Span
    assertThat(localParent.name()).isEqualTo("publish-batch");

    List<Span> messageSpans =
        reportedSpans.stream().filter(s -> s != localParent).collect(Collectors.toList());
    for (Span span : messageSpans) {
      verifyReportedPublishSpan(span);
    }

    assertThat(spans.size()).isEqualTo(0);
  }

  private void verifyInjectedTraceContext(Map<String, MessageAttributeValue> messageAttributes,
      Span parent) {
    TraceContextOrSamplingFlags extracted = extractor.extract(messageAttributes);

    assertThat(extracted.sampled()).isTrue();
    assertThat(extracted.traceIdContext()).isNull();
    assertThat(extracted.context()).isNotNull();

    if (parent == null) {
      assertThat(extracted.context().traceIdString()).isNotEmpty();
      assertThat(extracted.context().parentId()).isNull();
    } else {
      assertThat(extracted.context().traceIdString()).isEqualTo(parent.traceId());
      assertThat(extracted.context().parentIdString()).isEqualTo(parent.id());
    }
    assertThat(extracted.context().spanIdString()).isNotEmpty();
  }

  private void verifyReportedPublishSpan(Span span) {
    assertThat(span.kind()).isEqualTo(Span.Kind.PRODUCER);
    assertThat(span.name()).isEqualTo("publish");
    assertThat(span.remoteServiceName()).isEqualToIgnoringCase("amazon-sqs/queueUrl");
    assertThat(span.duration()).isEqualTo(1);
  }

  private Tracing.Builder tracingBuilder() {
    return Tracing.newBuilder()
        .spanReporter(spans::add)
        .currentTraceContext(
            ThreadLocalCurrentTraceContext.newBuilder()
                .addScopeDecorator(ThreadContextScopeDecorator.create()) // connect to log4j
                .addScopeDecorator(StrictScopeDecorator.create())
                .build())
        .sampler(Sampler.ALWAYS_SAMPLE);
  }
}
