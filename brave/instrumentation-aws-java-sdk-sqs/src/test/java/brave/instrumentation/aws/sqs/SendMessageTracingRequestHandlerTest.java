/*
 * Copyright 2016-2024 The OpenZipkin Authors
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
import brave.handler.MutableSpan;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.test.TestSpanHandler;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static brave.Span.Kind.PRODUCER;
import static org.assertj.core.api.Assertions.assertThat;

class SendMessageTracingRequestHandlerTest {
  TestSpanHandler spans = new TestSpanHandler();
  Tracing tracing = Tracing.newBuilder().addSpanHandler(spans).build();
  Extractor<Map<String, MessageAttributeValue>> extractor =
      tracing.propagation().extractor(SendMessageTracingRequestHandler.GETTER);
  SendMessageTracingRequestHandler handler = new SendMessageTracingRequestHandler(tracing);

  @AfterEach void cleanup() {
    tracing.close();
  }

  @Test void handleSendMessageRequest() {
    SendMessageRequest request = new SendMessageRequest("queueUrl", "test message content");

    handler.beforeExecution(request);

    // Verify propagation
    verifyInjectedTraceContext(request.getMessageAttributes(), null);

    // Verify Span
    assertThat(spans).hasSize(1);
    MutableSpan reportedSpan = spans.get(0);
    verifyReportedPublishSpan(reportedSpan);
  }

  @Test void handleSendMessageBatchRequest() {
    SendMessageBatchRequest request = new SendMessageBatchRequest("queueUrl");
    SendMessageBatchRequestEntry entry1 =
        new SendMessageBatchRequestEntry("id1", "test message body 1");
    SendMessageBatchRequestEntry entry2 =
        new SendMessageBatchRequestEntry("id2", "test message body 2");
    request.withEntries(entry1, entry2);

    handler.beforeExecution(request);

    assertThat(spans).hasSize(3);

    MutableSpan localParent =
        spans.spans().stream().filter(s -> s.parentId() == null).findFirst().get();

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
}
