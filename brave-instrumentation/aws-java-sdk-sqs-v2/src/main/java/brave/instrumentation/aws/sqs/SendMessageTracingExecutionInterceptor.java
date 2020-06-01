/*
 * Copyright 2016-2020 The OpenZipkin Authors
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

import brave.Span;
import brave.Tracing;
import brave.instrumentation.aws.sqs.propogation.SendMessageRemoteSetter;
import brave.propagation.TraceContext;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

/**
 * {@link ExecutionInterceptor} that will add tracing information to the message attributes of a SQS
 * message placed onto the queue.
 *
 * <p>This will allow the trace to be continued by all consuming services.
 */
public class SendMessageTracingExecutionInterceptor implements ExecutionInterceptor {
  static final ExecutionAttribute<Span> SPAN_EXECUTION_ATTRIBUTE = new ExecutionAttribute<>("span");

  private final Tracing tracing;
  private final TraceContext.Injector<Map<String, MessageAttributeValue>> messageAttributeInjector;
  private final SpanDecorator spanDecorator;

  public SendMessageTracingExecutionInterceptor(final Tracing tracing) {
    this(tracing, SpanDecorator.DEFAULT);
  }

  public SendMessageTracingExecutionInterceptor(final Tracing tracing,
      final SpanDecorator spanDecorator) {
    this(tracing, spanDecorator, SendMessageRemoteSetter.create(tracing));
  }

  public SendMessageTracingExecutionInterceptor(final Tracing tracing,
      final SpanDecorator spanDecorator,
      final TraceContext.Injector<Map<String, MessageAttributeValue>> injector) {
    this.tracing = tracing;
    this.spanDecorator = spanDecorator;
    this.messageAttributeInjector = injector;
  }

  @Override
  public void beforeExecution(final Context.BeforeExecution context,
      final ExecutionAttributes executionAttributes) {
    if (!(context.request() instanceof SendMessageRequest)) {
      return;
    }

    final Span span = tracing.tracer().nextSpan();

    if (!span.isNoop()) {
      spanDecorator.decorateMessageSpan((SendMessageRequest) context.request(), span);
    }
    span.start();

    executionAttributes.putAttribute(SPAN_EXECUTION_ATTRIBUTE, span);
  }

  @Override
  public SdkRequest modifyRequest(final Context.ModifyRequest context,
      final ExecutionAttributes executionAttributes) {
    if (!(context.request() instanceof SendMessageRequest)) {
      return context.request();
    }
    final SendMessageRequest request = (SendMessageRequest) context.request();

    final Span span = executionAttributes.getAttribute(SPAN_EXECUTION_ATTRIBUTE);
    if (span == null) {
      // someone deleted our attribute...
      return request;
    }

    final Map<String, MessageAttributeValue> currentMessageAttributes =
        new HashMap<>(request.messageAttributes());
    messageAttributeInjector.inject(span.context(), currentMessageAttributes);

    return request.toBuilder()
        .messageAttributes(currentMessageAttributes)
        .build();
  }

  @Override
  public void afterExecution(final Context.AfterExecution context,
      final ExecutionAttributes executionAttributes) {
    if (!(context.request() instanceof SendMessageRequest)) {
      return;
    }

    final Span span = executionAttributes.getAttribute(SPAN_EXECUTION_ATTRIBUTE);
    if (span == null) {
      // someone deleted our attribute...
      return;
    }

    try {
      final SendMessageRequest request = (SendMessageRequest) context.request();
      final SendMessageResponse response = (SendMessageResponse) context.response();
      if (context.httpResponse().isSuccessful()) {
        spanDecorator.decorateMessageSpanOnSuccess(request, response,
            context.httpResponse(), span);
      } else {
        spanDecorator.decorateMessageSpanOnFailure(request, response,
            context.httpResponse(), span);
        span.error(new RuntimeException("Error placing message onto SQS queue"));
      }
    } finally {
      span.finish();
    }
  }

  /**
   * Decorator that can be used to override the default span configurations, otherwise the {@link
   * SendMessageTracingExecutionInterceptor.SpanDecorator#DEFAULT} can be used.
   *
   * <p>This would be helpful if you want to send extra information or remove some of the tags
   * being sent as they are not relevant for your use case.
   */
  @SuppressWarnings("unused") public interface SpanDecorator {
    SpanDecorator DEFAULT = new SpanDecorator() {
    };

    /**
     * Decorate the message span before the message is sent to SQS.
     *
     * @param request the original request
     * @param span    the span corresponding to this message
     */
    default void decorateMessageSpan(final SendMessageRequest request,
        final Span span) {
      span.kind(Span.Kind.PRODUCER);
      span.name("sqs-send-message");
      span.remoteServiceName("aws-sqs");
      span.tag("queue.url", request.queueUrl());
    }

    /**
     * Decorate the message span when the message was successfully published to SQS.
     *
     * @param request         the request published to SQS
     * @param response        the response received
     * @param sdkHttpResponse the underlying HTTP response
     * @param span            the span for this message to decorate
     */
    default void decorateMessageSpanOnSuccess(final SendMessageRequest request,
        final SendMessageResponse response,
        final SdkHttpResponse sdkHttpResponse,
        final Span span) {
      span.tag("message.id", response.messageId());
    }

    /**
     * Decorate the message span when the message failed to be published to SQS.
     *
     * @param request         the request published to SQS
     * @param response        the response received
     * @param sdkHttpResponse the underlying HTTP response
     * @param span            the span for this message to decorate
     */
    default void decorateMessageSpanOnFailure(final SendMessageRequest request,
        final SendMessageResponse response,
        final SdkHttpResponse sdkHttpResponse,
        final Span span) {
      span.tag("response.code", String.valueOf(sdkHttpResponse.statusCode()));
    }
  }
}
