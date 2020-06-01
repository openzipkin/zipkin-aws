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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;

import static java.util.stream.Collectors.toMap;

/**
 * {@link ExecutionInterceptor} that is used to create spans for the {@link SendMessageBatchRequest}
 * and each individual message in the request.
 *
 * <p>Information about each message's span is included in the message attributes of the message
 * for consumers to continue the trace.
 */
public class SendMessageBatchTracingExecutionInterceptor implements ExecutionInterceptor {
  static final ExecutionAttribute<Map<String, Span>> MESSAGE_SPANS_EXECUTION_ATTRIBUTE =
      new ExecutionAttribute<>("message-spans");

  private final Tracing tracing;
  private final SpanDecorator spanDecorator;
  private final TraceContext.Injector<Map<String, MessageAttributeValue>> messageAttributeInjector;

  public SendMessageBatchTracingExecutionInterceptor(final Tracing tracing) {
    this(tracing, SpanDecorator.DEFAULT);
  }

  public SendMessageBatchTracingExecutionInterceptor(final Tracing tracing,
      final SpanDecorator spanDecorator) {
    this(tracing, spanDecorator, SendMessageRemoteSetter.create(tracing));
  }

  public SendMessageBatchTracingExecutionInterceptor(final Tracing tracing,
      final SpanDecorator spanDecorator,
      final TraceContext.Injector<Map<String, MessageAttributeValue>> injector) {
    this.tracing = tracing;
    this.spanDecorator = spanDecorator;
    this.messageAttributeInjector = injector;
  }

  @Override
  public void beforeExecution(final Context.BeforeExecution context,
      final ExecutionAttributes executionAttributes) {
    if (!(context.request() instanceof SendMessageBatchRequest)) {
      return;
    }

    final SendMessageBatchRequest request = (SendMessageBatchRequest) context.request();
    final Map<String, Span> messageSpans = request.entries().stream()
        .collect(
            toMap(SendMessageBatchRequestEntry::id, entry -> startSpanForMessage(request, entry)));

    executionAttributes.putAttribute(MESSAGE_SPANS_EXECUTION_ATTRIBUTE, messageSpans);
  }

  @Override
  public SdkRequest modifyRequest(final Context.ModifyRequest context,
      final ExecutionAttributes executionAttributes) {
    if (!(context.request() instanceof SendMessageBatchRequest)) {
      return context.request();
    }

    final Map<String, Span> messageSpans =
        executionAttributes.getAttribute(MESSAGE_SPANS_EXECUTION_ATTRIBUTE);
    if (messageSpans == null) {
      // someone deleted our attribute...
      return context.request();
    }

    final SendMessageBatchRequest request = (SendMessageBatchRequest) context.request();
    final List<SendMessageBatchRequestEntry> updatedEntries = request.entries().stream()
        .map(requestEntry -> injectSpanInformationIntoMessage(requestEntry,
            messageSpans.get(requestEntry.id())))
        .collect(Collectors.toList());

    return request.toBuilder()
        .entries(updatedEntries)
        .build();
  }

  @Override
  public void afterExecution(final Context.AfterExecution context,
      final ExecutionAttributes executionAttributes) {
    if (!(context.request() instanceof SendMessageBatchRequest)) {
      return;
    }

    final SendMessageBatchRequest request = (SendMessageBatchRequest) context.request();

    final Map<String, Span> individualMessageSpans =
        executionAttributes.getAttribute(MESSAGE_SPANS_EXECUTION_ATTRIBUTE);
    if (individualMessageSpans == null) {
      // someone deleted our attribute...
      return;
    }

    if (!context.httpResponse().isSuccessful()) {
      individualMessageSpans.values()
          .forEach(
              span -> {
                try {
                  spanDecorator.decorateRequestFailedMessageSpan(request,
                      context.httpResponse(), span);
                  span.error(new RuntimeException("Error placing message onto SQS queue"));
                } finally {
                  span.finish();
                }
              });
      return;
    }

    final SendMessageBatchResponse response = (SendMessageBatchResponse) context.response();

    response.successful().forEach(result -> {
      final Span messageSpan = individualMessageSpans.get(result.id());
      if (messageSpan == null) {
        // for some reason the individual message's span cannot be found
        return;
      }

      try {
        spanDecorator.decorateMessageSuccessfulSpan(response, context.httpResponse(),
            result, messageSpan);
      } finally {
        messageSpan.finish();
      }
    });

    response.failed().forEach(result -> {
      final Span messageSpan = individualMessageSpans.get(result.id());
      if (messageSpan == null) {
        // for some reason the individual message's span cannot be found
        return;
      }

      try {
        spanDecorator.decorateMessageFailureSpan(response, context.httpResponse(), result,
            messageSpan);
        messageSpan.error(new RuntimeException("Error placing message onto SQS queue"));
      } finally {
        messageSpan.finish();
      }
    });
  }

  private Span startSpanForMessage(final SendMessageBatchRequest request,
      final SendMessageBatchRequestEntry requestEntry) {
    final Span messageSpan = tracing.tracer().nextSpan();
    if (!messageSpan.isNoop()) {
      spanDecorator.decorateMessageSpan(request, requestEntry, messageSpan);
    }

    messageSpan.start();
    return messageSpan;
  }

  private SendMessageBatchRequestEntry injectSpanInformationIntoMessage(
      final SendMessageBatchRequestEntry entry, final Span span) {
    final Map<String, MessageAttributeValue> currentMessageAttributes =
        new HashMap<>(entry.messageAttributes());

    messageAttributeInjector.inject(span.context(), currentMessageAttributes);

    return entry.toBuilder()
        .messageAttributes(currentMessageAttributes)
        .build();
  }

  /**
   * Decorator that can be used to override the default span configurations, otherwise the {@link
   * SpanDecorator#DEFAULT} can be used.
   *
   * <p>This would be helpful if you want to send extra information or remove some of the tags
   * being sent as they are not relevant for your use case.
   */
  @SuppressWarnings("unused")
  public interface SpanDecorator {
    SpanDecorator DEFAULT = new SpanDecorator() {
    };

    /**
     * Decorate the message span before the message is sent to SQS.
     *
     * @param request the original request
     * @param entry   the entry for the message being handled
     * @param span    the span corresponding to this message
     */
    default void decorateMessageSpan(final SendMessageBatchRequest request,
        final SendMessageBatchRequestEntry entry,
        final Span span) {
      span.kind(Span.Kind.PRODUCER);
      span.name("sqs-send-message-batch");
      span.remoteServiceName("aws-sqs");
      span.tag("queue.url", request.queueUrl());
      span.tag("message.request.id", entry.id());
    }

    /**
     * Decorator called for each message when the entire HTTP request to SQS fails.
     *
     * @param request      the request that was sent to SQS
     * @param httpResponse the http response that indicates the failure
     * @param span         the span to apply decorations to
     */
    default void decorateRequestFailedMessageSpan(final SendMessageBatchRequest request,
        final SdkHttpResponse httpResponse,
        final Span span) {
      span.tag("response.code", String.valueOf(httpResponse.statusCode()));
    }

    /**
     * Decorator called for each message that was successfully published to SQS.
     *
     * @param response     the response for the request
     * @param httpResponse the underlying http response
     * @param entry        the message entry that was a success
     * @param span         the span corresponding to this message
     */
    default void decorateMessageSuccessfulSpan(final SendMessageBatchResponse response,
        final SdkHttpResponse httpResponse,
        final SendMessageBatchResultEntry entry,
        final Span span) {
      span.tag("message.id", entry.messageId());
    }

    /**
     * Decorator called for each message that failed to be published to SQS.
     *
     * <p>Note that this is only called if the underlying HTTP was a success but individual
     * messages failed to be processed.
     *
     * @param response     the response for the request
     * @param httpResponse the underlying http response
     * @param entry        the message entry that was a failure
     * @param span         the span corresponding to this message
     */
    default void decorateMessageFailureSpan(final SendMessageBatchResponse response,
        final SdkHttpResponse httpResponse,
        final BatchResultErrorEntry entry,
        final Span span) {
    }
  }
}
