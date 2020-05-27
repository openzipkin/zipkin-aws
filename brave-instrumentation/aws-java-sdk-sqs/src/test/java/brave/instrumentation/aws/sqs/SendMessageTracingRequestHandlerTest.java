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
import brave.handler.MutableSpan;
import brave.propagation.StrictScopeDecorator;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.sampler.Sampler;
import brave.test.TestSpanHandler;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static brave.Span.Kind.PRODUCER;

public class SendMessageTracingRequestHandlerTest {
  private TestSpanHandler spans = new TestSpanHandler();

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
    assertThat(spans).hasSize(1);
    MutableSpan reportedSpan = spans.get(0);
    verifyReportedPublishSpan(reportedSpan);
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

    assertThat(spans).hasSize(3);

    MutableSpan localParent = spans.spans().stream().filter(s -> s.parentId() == null).findFirst().get();

    // Verify propagation
    for (SendMessageBatchRequestEntry entry : request.getEntries()) {
      verifyInjectedTraceContext(entry.getMessageAttributes(), localParent);
    }

    // Verify Span
    assertThat(localParent.name()).isEqualTo("publish-batch");

    List<MutableSpan> messageSpans =
        spans.spans().stream().filter(s -> s != localParent).collect(Collectors.toList());
    for (MutableSpan span : messageSpans) {
      verifyReportedPublishSpan(span);
    }
  }

  private void verifyInjectedTraceContext(Map<String, MessageAttributeValue> messageAttributes,
      MutableSpan parent) {
    TraceContextOrSamplingFlags extracted = extractor.extract(messageAttributes);

    assertThat(extracted.sampled()).isTrue();
    assertThat(extracted.traceIdContext()).isNull();
    assertThat(extracted.context()).isNotNull();

    if (parent == null) {
      assertThat(extracted.context().traceIdString()).isNotEmpty();
    } else {
      assertThat(extracted.context().traceIdString()).isEqualTo(parent.traceId());
    }
    assertThat(extracted.context().spanIdString()).isNotEmpty();
  }

  private void verifyReportedPublishSpan(MutableSpan span) {
    assertThat(span.kind()).isEqualTo(PRODUCER);
    assertThat(span.name()).isEqualTo("publish");
    assertThat(span.remoteServiceName()).isEqualToIgnoringCase("amazon-sqs");
    assertThat(span.tags().get("queue.url")).isEqualToIgnoringCase("queueUrl");
    assertThat(span.finishTimestamp() - span.startTimestamp()).isZero();
  }

  private Tracing.Builder tracingBuilder() {
    return Tracing.newBuilder()
        .addSpanHandler(spans)
        .currentTraceContext(
            ThreadLocalCurrentTraceContext.newBuilder()
                .addScopeDecorator(ThreadContextScopeDecorator.create()) // connect to log4j
                .addScopeDecorator(StrictScopeDecorator.create())
                .build())
        .sampler(Sampler.ALWAYS_SAMPLE);
  }
}
