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

import brave.ScopedSpan;
import brave.Span;
import brave.Tracing;
import brave.instrumentation.aws.sqs.propogation.SendMessageRemoteGetter;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.test.TestSpanHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;
import software.amazon.awssdk.utils.ImmutableMap;

import static brave.instrumentation.aws.sqs.SendMessageBatchTracingExecutionInterceptor.MESSAGE_SPANS_EXECUTION_ATTRIBUTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SendMessageBatchTracingExecutionInterceptorTest {
  private final TestSpanHandler spanHandler = new TestSpanHandler();
  private final Tracing tracing = Tracing.newBuilder()
      .addSpanHandler(spanHandler)
      .build();

  private SendMessageBatchTracingExecutionInterceptor interceptor;

  @Before
  public void setUp() {
    interceptor = new SendMessageBatchTracingExecutionInterceptor(tracing);
  }

  @After
  public void tearDown() {
    tracing.close();
  }

  @Test
  public void beforeExecutionWillPerformNoActionForNonSendMessageBatchRequests() {
    // arrange
    final DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
        .queueUrl("queueUrl")
        .receiptHandle("receiptHandle")
        .build();

    // act
    interceptor.beforeExecution(() -> deleteMessageRequest, new ExecutionAttributes());

    // assert
    assertThat(spanHandler.spans()).isEmpty();
  }

  @Test
  public void beforeExecutionWhenNoCurrentSpanANewOneWillBeCreatedForEachMessage() {
    // arrange
    final SendMessageBatchRequest request = SendMessageBatchRequest.builder()
        .queueUrl("queueUrl")
        .entries(SendMessageBatchRequestEntry.builder()
                .id("first")
                .messageBody("body")
                .build(),
            SendMessageBatchRequestEntry.builder()
                .id("second")
                .messageBody("body")
                .build()
        )
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();

    // act
    interceptor.beforeExecution(() -> request, executionAttributes);

    // assert
    final Map<String, Span> spans =
        executionAttributes.getAttribute(MESSAGE_SPANS_EXECUTION_ATTRIBUTE);
    assertThat(spans).hasSize(2);
    assertThat(spans).allSatisfy((entryId, span) -> assertThat(span.context().parentId()).isNull());
  }

  @Test
  public void beforeExecutionWhenASpanExistsTheMessageSpanWillBeAChildOfThisSpan() {
    // arrange
    final SendMessageBatchRequest request = SendMessageBatchRequest.builder()
        .queueUrl("queueUrl")
        .entries(SendMessageBatchRequestEntry.builder()
                .id("first")
                .messageBody("body")
                .build(),
            SendMessageBatchRequestEntry.builder()
                .id("second")
                .messageBody("body")
                .build()
        )
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();

    // act
    final ScopedSpan parentSpan = tracing.tracer().startScopedSpan("parent");
    try {
      interceptor.beforeExecution(() -> request, executionAttributes);
    } finally {
      parentSpan.finish();
    }

    // assert
    final Map<String, Span> spans =
        executionAttributes.getAttribute(MESSAGE_SPANS_EXECUTION_ATTRIBUTE);
    assertThat(spans).hasSize(2);
    assertThat(spans).allSatisfy((entryId, span) -> assertThat(span.context().parentId()).isEqualTo(
        parentSpan.context().spanId()));
  }

  @Test
  public void beforeExecutionMessageSpansWillBePopulatedWithDefaultQueueInformation() {
    // arrange
    final SendMessageBatchRequest request = SendMessageBatchRequest.builder()
        .queueUrl("queueUrl")
        .entries(SendMessageBatchRequestEntry.builder()
                .id("first")
                .messageBody("body")
                .build(),
            SendMessageBatchRequestEntry.builder()
                .id("second")
                .messageBody("body")
                .build()
        )
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();

    // act
    interceptor.beforeExecution(() -> request, executionAttributes);
    final Map<String, Span> spans =
        executionAttributes.getAttribute(MESSAGE_SPANS_EXECUTION_ATTRIBUTE);
    spans.values().forEach(Span::finish);

    // assert
    assertThat(spanHandler.spans()).allSatisfy((span) -> {
      assertThat(span.kind()).isEqualTo(Span.Kind.PRODUCER);
      assertThat(span.name()).isEqualTo("sqs-send-message-batch");
      assertThat(span.remoteServiceName()).isEqualTo("aws-sqs");
      assertThat(span.tag("queue.url")).isEqualTo("queueUrl");
    });
  }

  @Test
  public void beforeExecutionRequestSpanInformationCanBeOverwritten() {
    // arrange
    final SendMessageBatchRequest request = SendMessageBatchRequest.builder()
        .queueUrl("queueUrl")
        .entries(SendMessageBatchRequestEntry.builder()
                .id("first")
                .messageBody("body")
                .build(),
            SendMessageBatchRequestEntry.builder()
                .id("second")
                .messageBody("body")
                .build()
        )
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    final SendMessageBatchTracingExecutionInterceptor.SpanDecorator spanDecorator =
        new SendMessageBatchTracingExecutionInterceptor.SpanDecorator() {
          @Override public void decorateMessageSpan(SendMessageBatchRequest request,
              SendMessageBatchRequestEntry entry, Span span) {
            span.tag("test", "value");
          }
        };

    // act
    final SendMessageBatchTracingExecutionInterceptor interceptor =
        new SendMessageBatchTracingExecutionInterceptor(tracing, spanDecorator);
    interceptor.beforeExecution(() -> request, executionAttributes);
    final Map<String, Span> spans =
        executionAttributes.getAttribute(MESSAGE_SPANS_EXECUTION_ATTRIBUTE);
    spans.values().forEach(Span::finish);

    // assert
    assertThat(spanHandler.spans()).allSatisfy((span) -> {
      assertThat(span.name()).isNull();
      assertThat(span.remoteServiceName()).isNull();
      assertThat(span.tag("test")).isEqualTo("value");
    });
  }

  @Test
  public void modifyRequestWillNotUpdateRequestIfNotSendMessageRequest() {
    // arrange
    final DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
        .queueUrl("queueUrl")
        .receiptHandle("receiptHandle")
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();

    // act
    final SdkRequest newRequest =
        interceptor.modifyRequest(() -> deleteMessageRequest, executionAttributes);

    // assert
    assertThat(newRequest).isSameAs(deleteMessageRequest);
  }

  @Test
  public void modifyRequestWillAddSpanInformationToMessageAttributes() {
    // arrange
    final SendMessageBatchRequest request = SendMessageBatchRequest.builder()
        .queueUrl("queueUrl")
        .entries(SendMessageBatchRequestEntry.builder()
                .id("first")
                .messageBody("body")
                .build(),
            SendMessageBatchRequestEntry.builder()
                .id("second")
                .messageBody("body")
                .build()
        )
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    final TraceContext.Extractor<Map<String, MessageAttributeValue>> extractor =
        SendMessageRemoteGetter.create(tracing);

    // act
    final SendMessageBatchRequest newRequest =
        (SendMessageBatchRequest) interceptor.modifyRequest(() -> request, executionAttributes);
    final Map<String, Span> spans =
        executionAttributes.getAttribute(MESSAGE_SPANS_EXECUTION_ATTRIBUTE);
    spans.values().forEach(Span::finish);

    // assert
    assertThat(newRequest.entries()).allSatisfy((entry) -> {
      assertThat(entry.messageAttributes()).containsKeys("b3");
      final TraceContextOrSamplingFlags traceContextOrSamplingFlags =
          extractor.extract(entry.messageAttributes());
      assertThat(traceContextOrSamplingFlags).isNotNull();
      final Span entrySpan = spans.get(entry.id());
      assertThat(traceContextOrSamplingFlags.context().traceIdString()).isEqualTo(
          entrySpan.context().traceIdString());
      assertThat(traceContextOrSamplingFlags.context().spanIdString()).isEqualTo(
          entrySpan.context().spanIdString());
    });
  }

  @Test
  public void modifyRequestWhenSpanDeletedFromExecutionContextWillReturnOriginalRequest() {
    // arrange
    final SendMessageBatchRequest request = SendMessageBatchRequest.builder()
        .queueUrl("queueUrl")
        .entries(SendMessageBatchRequestEntry.builder()
                .id("first")
                .messageBody("body")
                .build(),
            SendMessageBatchRequestEntry.builder()
                .id("second")
                .messageBody("body")
                .build()
        )
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    executionAttributes.putAttribute(MESSAGE_SPANS_EXECUTION_ATTRIBUTE, null);

    // act
    final SendMessageBatchRequest newRequest =
        (SendMessageBatchRequest) interceptor.modifyRequest(() -> request, executionAttributes);

    // assert
    assertThat(newRequest).isSameAs(request);
  }

  @Test
  public void afterExecutionWillDoNothingIfNotSendMessageRequest() {
    // arrange
    final DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
        .queueUrl("queueUrl")
        .receiptHandle("receiptHandle")
        .build();
    final Context.AfterExecution afterExecution =
        mockAfterExecutionFailure(deleteMessageRequest, 400);
    final Span span = tracing.tracer().nextSpan();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    executionAttributes.putAttribute(MESSAGE_SPANS_EXECUTION_ATTRIBUTE,
        ImmutableMap.of("test", span));

    // act
    interceptor.afterExecution(afterExecution, executionAttributes);

    // assert
    assertThat(spanHandler.spans()).isEmpty();
  }

  @Test
  public void afterExecutionWillDoNothingIfNotSpanIsNotPresentInExecutionAttributes() {
    // arrange
    final SendMessageBatchRequest request = SendMessageBatchRequest.builder()
        .queueUrl("queueUrl")
        .entries(SendMessageBatchRequestEntry.builder()
                .id("first")
                .messageBody("body")
                .build(),
            SendMessageBatchRequestEntry.builder()
                .id("second")
                .messageBody("body")
                .build()
        )
        .build();
    final Context.AfterExecution afterExecution = mockAfterExecutionFailure(request, 400);
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    executionAttributes.putAttribute(MESSAGE_SPANS_EXECUTION_ATTRIBUTE, null);

    // act
    interceptor.afterExecution(afterExecution, executionAttributes);

    // assert
    assertThat(spanHandler.spans()).isEmpty();
  }

  @Test
  public void afterExecutionWillFinishSpan() {
    // arrange
    final SendMessageBatchRequest request = SendMessageBatchRequest.builder()
        .queueUrl("queueUrl")
        .entries(SendMessageBatchRequestEntry.builder()
                .id("first")
                .messageBody("body")
                .build(),
            SendMessageBatchRequestEntry.builder()
                .id("second")
                .messageBody("body")
                .build()
        )
        .build();
    final List<SendMessageBatchResultEntry> successfulMessages = new ArrayList<>();
    successfulMessages.add(SendMessageBatchResultEntry.builder()
        .id("first")
        .messageId("first-message-id")
        .build());
    successfulMessages.add(SendMessageBatchResultEntry.builder()
        .id("second")
        .messageId("second-message-id")
        .build());
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    final Context.AfterExecution afterExecution = mockAfterExecutionSuccess(request,
        successfulMessages, new ArrayList<>());

    // act
    interceptor.afterExecution(afterExecution, executionAttributes);

    // assert
    assertThat(spanHandler.spans()).isNotEmpty();
  }

  @Test
  public void afterExecutionSuccessfulPublishingOfMessageWillIncludeMessageIdTag() {
    // arrange
    final SendMessageBatchRequest request = SendMessageBatchRequest.builder()
        .queueUrl("queueUrl")
        .entries(SendMessageBatchRequestEntry.builder()
                .id("first")
                .messageBody("body")
                .build(),
            SendMessageBatchRequestEntry.builder()
                .id("second")
                .messageBody("body")
                .build()
        )
        .build();
    final List<SendMessageBatchResultEntry> successfulMessages = new ArrayList<>();
    successfulMessages.add(SendMessageBatchResultEntry.builder()
        .id("first")
        .messageId("first-message-id")
        .build());
    successfulMessages.add(SendMessageBatchResultEntry.builder()
        .id("second")
        .messageId("second-message-id")
        .build());
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    final Context.AfterExecution afterExecution = mockAfterExecutionSuccess(request,
        successfulMessages, new ArrayList<>());
    final Map<String, Span> spans =
        executionAttributes.getAttribute(MESSAGE_SPANS_EXECUTION_ATTRIBUTE);
    spans.values().forEach(Span::finish);

    // act
    interceptor.afterExecution(afterExecution, executionAttributes);

    // assert
    assertThat(spanHandler.spans()).hasSize(2);
    assertThat(spanHandler.spans()).anyMatch(span ->
        span.tag("message.request.id").equals("first") && span.tag("message.id")
            .equals("first-message-id"));
    assertThat(spanHandler.spans()).anyMatch(span ->
        span.tag("message.request.id").equals("second") && span.tag("message.id")
            .equals("second-message-id"));
  }

  @Test
  public void afterExecutionOnFailureWillErrorOutTheSpan() {
    // arrange
    final SendMessageBatchRequest request = SendMessageBatchRequest.builder()
        .queueUrl("queueUrl")
        .entries(SendMessageBatchRequestEntry.builder()
                .id("first")
                .messageBody("body")
                .build(),
            SendMessageBatchRequestEntry.builder()
                .id("second")
                .messageBody("body")
                .build()
        )
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    final Context.AfterExecution afterExecution = mockAfterExecutionFailure(request, 400);

    // act
    interceptor.afterExecution(afterExecution, executionAttributes);

    // assert
    assertThat(spanHandler.spans()).hasSize(2);
    assertThat(spanHandler.spans()).allSatisfy(span -> {
      assertThat(span.error()).hasMessage("Error placing message onto SQS queue");
      assertThat(span.tag("response.code")).isEqualTo("400");
    });
  }

  @Test
  public void afterExecutionCustomSpanDecoratorWillRunAfterExecutionForFailures() {
    // arrange
    final SendMessageBatchRequest request = SendMessageBatchRequest.builder()
        .queueUrl("queueUrl")
        .entries(SendMessageBatchRequestEntry.builder()
                .id("first")
                .messageBody("body")
                .build(),
            SendMessageBatchRequestEntry.builder()
                .id("second")
                .messageBody("body")
                .build()
        )
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    final Context.AfterExecution afterExecution = mockAfterExecutionFailure(request, 500);
    final SendMessageBatchTracingExecutionInterceptor.SpanDecorator spanDecorator =
        new SendMessageBatchTracingExecutionInterceptor.SpanDecorator() {
          @Override
          public void decorateRequestFailedMessageSpan(SendMessageBatchRequest request,
              SdkHttpResponse httpResponse, Span span) {
            span.tag("test", "value");
          }
        };

    // act
    final SendMessageBatchTracingExecutionInterceptor interceptor =
        new SendMessageBatchTracingExecutionInterceptor(tracing, spanDecorator);
    interceptor.afterExecution(afterExecution, executionAttributes);

    // assert
    assertThat(spanHandler.spans()).hasSize(2);
    assertThat(spanHandler.spans()).allSatisfy(
        span -> assertThat(span.tag("test")).isEqualTo("value"));
  }

  @Test
  public void failureToProcessIndividualMessagesWillErrorOutThatSpan() {
    // arrange
    final SendMessageBatchRequest request = SendMessageBatchRequest.builder()
        .queueUrl("queueUrl")
        .entries(SendMessageBatchRequestEntry.builder()
                .id("first")
                .messageBody("body")
                .build(),
            SendMessageBatchRequestEntry.builder()
                .id("second")
                .messageBody("body")
                .build()
        )
        .build();
    final List<SendMessageBatchResultEntry> successfulMessages = new ArrayList<>();
    successfulMessages.add(SendMessageBatchResultEntry.builder()
        .id("first")
        .messageId("first-message-id")
        .build());
    final List<BatchResultErrorEntry> failingMessages = new ArrayList<>();
    failingMessages.add(BatchResultErrorEntry.builder()
        .id("second")
        .code("some error code")
        .build());
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    final Context.AfterExecution afterExecution = mockAfterExecutionSuccess(request,
        successfulMessages, failingMessages);
    final Map<String, Span> spans =
        executionAttributes.getAttribute(MESSAGE_SPANS_EXECUTION_ATTRIBUTE);
    spans.values().forEach(Span::finish);

    // act
    interceptor.afterExecution(afterExecution, executionAttributes);

    // assert
    assertThat(spanHandler.spans()).hasSize(2);
    assertThat(spanHandler.spans()).anyMatch(span ->
        span.tag("message.request.id").equals("first") && span.tag("message.id")
            .equals("first-message-id"));
    assertThat(spanHandler.spans()).anySatisfy(span -> {
      assertThat(span.tag("message.request.id")).isEqualTo("second");
      assertThat(span.error()).hasMessage("Error placing message onto SQS queue");
    });
  }

  private Context.AfterExecution mockAfterExecutionSuccess(final SdkRequest request,
      final Collection<SendMessageBatchResultEntry> successful,
      final Collection<BatchResultErrorEntry> failed) {
    final Context.AfterExecution afterExecution = mock(Context.AfterExecution.class);
    final SdkHttpResponse sdkHttpResponse = mock(SdkHttpResponse.class);
    when(afterExecution.httpResponse()).thenReturn(sdkHttpResponse);
    when(afterExecution.request()).thenReturn(request);
    when(sdkHttpResponse.isSuccessful()).thenReturn(true);
    when(afterExecution.response()).thenReturn(SendMessageBatchResponse.builder()
        .successful(successful)
        .failed(failed)
        .build());
    return afterExecution;
  }

  private Context.AfterExecution mockAfterExecutionFailure(final SdkRequest request,
      final int statusCode) {
    final Context.AfterExecution afterExecution = mock(Context.AfterExecution.class);
    final SdkHttpResponse sdkHttpResponse = mock(SdkHttpResponse.class);
    when(afterExecution.httpResponse()).thenReturn(sdkHttpResponse);
    when(afterExecution.request()).thenReturn(request);
    when(sdkHttpResponse.isSuccessful()).thenReturn(false);
    when(sdkHttpResponse.statusCode()).thenReturn(statusCode);
    return afterExecution;
  }
}