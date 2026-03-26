/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package brave.instrumentation.awssdk.sqs;

import brave.Tracing;
import brave.handler.MutableSpan;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.test.TestSpanHandler;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static brave.Span.Kind.PRODUCER;
import static org.assertj.core.api.Assertions.assertThat;

class SendMessageTracingExecutionInterceptorTest {
  TestSpanHandler spans = new TestSpanHandler();
  Tracing tracing = Tracing.newBuilder().addSpanHandler(spans).build();
  Extractor<Map<String, MessageAttributeValue>> extractor =
      tracing.propagation().extractor(SendMessageTracingExecutionInterceptor.GETTER);
  SendMessageTracingExecutionInterceptor interceptor =
      new SendMessageTracingExecutionInterceptor(tracing);

  @AfterEach void cleanup() {
    tracing.close();
  }

  @Test void handleSendMessageRequest() {
    SendMessageRequest request = SendMessageRequest.builder()
        .queueUrl("queueUrl")
        .messageBody("test message content")
        .build();

    SdkRequest modified = interceptor.modifyRequest(
        modifyRequestContext(request), ExecutionAttributes.builder().build());

    // Verify propagation
    SendMessageRequest result = (SendMessageRequest) modified;
    verifyInjectedTraceContext(result.messageAttributes(), null);

    // Verify Span
    assertThat(spans).hasSize(1);
    MutableSpan reportedSpan = spans.get(0);
    verifyReportedPublishSpan(reportedSpan);
  }

  @Test void handleSendMessageBatchRequest() {
    SendMessageBatchRequest request = SendMessageBatchRequest.builder()
        .queueUrl("queueUrl")
        .entries(
            SendMessageBatchRequestEntry.builder()
                .id("id1").messageBody("test message body 1").build(),
            SendMessageBatchRequestEntry.builder()
                .id("id2").messageBody("test message body 2").build())
        .build();

    SdkRequest modified = interceptor.modifyRequest(
        modifyRequestContext(request), ExecutionAttributes.builder().build());

    assertThat(spans).hasSize(3);

    MutableSpan localParent =
        spans.spans().stream().filter(s -> s.parentId() == null).findFirst().get();

    // Verify propagation
    SendMessageBatchRequest result = (SendMessageBatchRequest) modified;
    for (SendMessageBatchRequestEntry entry : result.entries()) {
      verifyInjectedTraceContext(entry.messageAttributes(), localParent);
    }

    // Verify Span
    assertThat(localParent.kind()).isEqualTo(PRODUCER);
    assertThat(localParent.name()).isEqualTo("publish-batch");
    assertThat(localParent.remoteServiceName()).isEqualToIgnoringCase("amazon-sqs");

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

  static Context.ModifyRequest modifyRequestContext(SdkRequest request) {
    return () -> request;
  }
}
