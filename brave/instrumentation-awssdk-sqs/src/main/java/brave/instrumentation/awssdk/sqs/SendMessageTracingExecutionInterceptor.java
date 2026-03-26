/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package brave.instrumentation.awssdk.sqs;

import brave.Span;
import brave.Tracer;
import brave.Tracer.SpanInScope;
import brave.Tracing;
import brave.propagation.CurrentTraceContext;
import brave.propagation.Propagation.RemoteGetter;
import brave.propagation.Propagation.RemoteSetter;
import brave.propagation.TraceContext;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.propagation.TraceContextOrSamplingFlags;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static brave.Span.Kind.PRODUCER;

final class SendMessageTracingExecutionInterceptor implements ExecutionInterceptor {

  static final RemoteSetter<Map<String, MessageAttributeValue>> SETTER =
      new RemoteSetter<Map<String, MessageAttributeValue>>() {
        @Override public Span.Kind spanKind() {
          return PRODUCER;
        }

        @Override public void put(Map<String, MessageAttributeValue> carrier, String key,
            String value) {
          carrier.put(key,
              MessageAttributeValue.builder().dataType("String").stringValue(value).build());
        }
      };

  static final RemoteGetter<Map<String, MessageAttributeValue>> GETTER =
      new RemoteGetter<Map<String, MessageAttributeValue>>() {
        @Override public Span.Kind spanKind() {
          return PRODUCER;
        }

        @Override public String get(Map<String, MessageAttributeValue> carrier, String key) {
          MessageAttributeValue value = carrier.get(key);
          return value != null ? value.stringValue() : null;
        }
      };

  final Tracing tracing;
  final Tracer tracer;
  final CurrentTraceContext currentTraceContext;
  final Injector<Map<String, MessageAttributeValue>> injector;
  final Extractor<Map<String, MessageAttributeValue>> extractor;

  SendMessageTracingExecutionInterceptor(Tracing tracing) {
    this.tracing = tracing;
    this.tracer = tracing.tracer();
    this.currentTraceContext = tracing.currentTraceContext();
    this.injector = tracing.propagation().injector(SETTER);
    this.extractor = tracing.propagation().extractor(GETTER);
  }

  @Override
  public SdkRequest modifyRequest(Context.ModifyRequest context,
      ExecutionAttributes executionAttributes) {
    SdkRequest request = context.request();
    if (request instanceof SendMessageRequest) {
      return handleSendMessageRequest((SendMessageRequest) request);
    } else if (request instanceof SendMessageBatchRequest) {
      return handleSendMessageBatchRequest((SendMessageBatchRequest) request);
    }
    return request;
  }

  private SdkRequest handleSendMessageRequest(SendMessageRequest request) {
    Map<String, MessageAttributeValue> mutableAttributes =
        new LinkedHashMap<>(request.messageAttributes());
    injectPerMessage(request.queueUrl(), mutableAttributes);
    return request.toBuilder().messageAttributes(mutableAttributes).build();
  }

  private SdkRequest handleSendMessageBatchRequest(SendMessageBatchRequest request) {
    TraceContext maybeParent = currentTraceContext.get();

    Span span;
    if (maybeParent == null) {
      span = tracer.nextSpan();
    } else {
      // If we have a span in scope assume headers were cleared before
      span = tracer.newChild(maybeParent);
    }

    span.name("publish-batch").kind(PRODUCER).remoteServiceName("amazon-sqs").start();
    List<SendMessageBatchRequestEntry> modifiedEntries;
    try (SpanInScope scope = tracer.withSpanInScope(span)) {
      modifiedEntries = new ArrayList<>(request.entries().size());
      for (SendMessageBatchRequestEntry entry : request.entries()) {
        Map<String, MessageAttributeValue> mutableAttributes =
            new LinkedHashMap<>(entry.messageAttributes());
        injectPerMessage(request.queueUrl(), mutableAttributes);
        modifiedEntries.add(
            entry.toBuilder().messageAttributes(mutableAttributes).build());
      }
    } finally {
      span.finish();
    }

    return request.toBuilder().entries(modifiedEntries).build();
  }

  private void injectPerMessage(String queueUrl,
      Map<String, MessageAttributeValue> messageAttributes) {
    TraceContext maybeParent = currentTraceContext.get();

    Span span;
    if (maybeParent == null) {
      span = tracer.nextSpan(extractAndClearHeaders(messageAttributes));
    } else {
      // If we have a span in scope assume headers were cleared before
      span = tracer.newChild(maybeParent);
    }

    if (!span.isNoop()) {
      span.kind(PRODUCER).name("publish");
      span.remoteServiceName("amazon-sqs");
      span.tag("queue.url", queueUrl);
      // incur timestamp overhead only once
      long timestamp = tracing.clock(span.context()).currentTimeMicroseconds();
      span.start(timestamp).finish(timestamp);
    }

    injector.inject(span.context(), messageAttributes);
  }

  private TraceContextOrSamplingFlags extractAndClearHeaders(
      Map<String, MessageAttributeValue> messageAttributes) {
    TraceContextOrSamplingFlags extracted = extractor.extract(messageAttributes);

    for (String propagationKey : tracing.propagation().keys()) {
      messageAttributes.remove(propagationKey);
    }

    return extracted;
  }
}
