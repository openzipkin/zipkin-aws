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
import brave.handler.MutableSpan;
import brave.instrumentation.aws.sqs.propogation.SendMessageRemoteGetter;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.test.TestSpanHandler;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import static brave.instrumentation.aws.sqs.SendMessageTracingExecutionInterceptor.SPAN_EXECUTION_ATTRIBUTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SendMessageTracingExecutionInterceptorTest {
  private final TestSpanHandler spanHandler = new TestSpanHandler();
  private final Tracing tracing = Tracing.newBuilder()
      .addSpanHandler(spanHandler)
      .build();

  private SendMessageTracingExecutionInterceptor interceptor;

  @Before
  public void setUp() {
    interceptor = new SendMessageTracingExecutionInterceptor(tracing);
  }

  @After
  public void tearDown() {
    tracing.close();
  }

  @Test
  public void beforeExecutionWillPerformNoActionForNonSendMessageRequests() {
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
  public void beforeExecutionWhenNoCurrentSpanANewOneWillBeCreatedForMessage() {
    // arrange
    final SendMessageRequest request = SendMessageRequest.builder()
        .queueUrl("queueUrl")
        .messageBody("body")
        .messageAttributes(new HashMap<>())
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();

    // act
    interceptor.beforeExecution(() -> request, executionAttributes);

    // assert
    final Span span = executionAttributes.getAttribute(SPAN_EXECUTION_ATTRIBUTE);
    assertThat(span).isNotNull();
    assertThat(span.context().parentId()).isNull();
  }

  @Test
  public void beforeExecutionWhenASpanExistsTheSendMessageSpanWillBeAChildOfThisSpan() {
    // arrange
    final SendMessageRequest request = SendMessageRequest.builder()
        .queueUrl("queueUrl")
        .messageBody("body")
        .messageAttributes(new HashMap<>())
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
    final Span span = executionAttributes.getAttribute(SPAN_EXECUTION_ATTRIBUTE);
    assertThat(span).isNotNull();
    assertThat(span.context().parentId()).isEqualTo(parentSpan.context().spanId());
  }

  @Test
  public void beforeExecutionSendMessageSpanWillBePopulatedWithDefaultQueueInformation() {
    // arrange
    final SendMessageRequest request = SendMessageRequest.builder()
        .queueUrl("queueUrl")
        .messageBody("body")
        .messageAttributes(new HashMap<>())
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();

    // act
    interceptor.beforeExecution(() -> request, executionAttributes);
    final Span span = executionAttributes.getAttribute(SPAN_EXECUTION_ATTRIBUTE);
    span.finish();

    // assert
    final MutableSpan sendMessageMutableSpan = spanHandler.spans().get(0);
    assertThat(sendMessageMutableSpan.kind()).isEqualTo(Span.Kind.PRODUCER);
    assertThat(sendMessageMutableSpan.name()).isEqualTo("sqs-send-message");
    assertThat(sendMessageMutableSpan.remoteServiceName()).isEqualTo("aws-sqs");
    assertThat(sendMessageMutableSpan.tag("queue.url")).isEqualTo("queueUrl");
  }

  @Test
  public void beforeExecutionRequestSpanInformationCanBeOverwritten() {
    // arrange
    final SendMessageRequest request = SendMessageRequest.builder()
        .queueUrl("queueUrl")
        .messageBody("body")
        .messageAttributes(new HashMap<>())
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    final SendMessageTracingExecutionInterceptor.SpanDecorator spanDecorator =
        new SendMessageTracingExecutionInterceptor.SpanDecorator() {
          @Override
          public void decorateMessageSpan(SendMessageRequest request, Span span) {
            span.tag("test", "value");
          }
        };

    // act
    final SendMessageTracingExecutionInterceptor interceptor =
        new SendMessageTracingExecutionInterceptor(tracing, spanDecorator);
    interceptor.beforeExecution(() -> request, executionAttributes);
    final Span span = executionAttributes.getAttribute(SPAN_EXECUTION_ATTRIBUTE);
    span.finish();

    // assert
    final MutableSpan sendMessageMutableSpan = spanHandler.spans().get(0);
    assertThat(sendMessageMutableSpan.name()).isNull();
    assertThat(sendMessageMutableSpan.remoteServiceName()).isNull();
    assertThat(sendMessageMutableSpan.tag("test")).isEqualTo("value");
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
    final SendMessageRequest request = SendMessageRequest.builder()
        .queueUrl("queueUrl")
        .messageBody("body")
        .messageAttributes(new HashMap<>())
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);

    // act
    final SendMessageRequest newRequest =
        (SendMessageRequest) interceptor.modifyRequest(() -> request, executionAttributes);
    final Span span = executionAttributes.getAttribute(SPAN_EXECUTION_ATTRIBUTE);
    span.finish();

    // assert
    assertThat(newRequest.messageAttributes()).containsKeys("b3");
    final TraceContext.Extractor<Map<String, MessageAttributeValue>> attributeExtractor =
        tracing.propagation().extractor(new SendMessageRemoteGetter());
    final TraceContextOrSamplingFlags traceContextOrSamplingFlags =
        attributeExtractor.extract(newRequest.messageAttributes());
    final MutableSpan sendMessageMutableSpan = spanHandler.get(0);
    assertThat(traceContextOrSamplingFlags).isNotNull();
    assertThat(traceContextOrSamplingFlags.context().traceIdString()).isEqualTo(
        sendMessageMutableSpan.traceId());
    assertThat(traceContextOrSamplingFlags.context().spanIdString()).isEqualTo(
        sendMessageMutableSpan.id());
  }

  @Test
  public void modifyRequestWhenSpanDeletedFromExecutionContextWillReturnOriginalRequest() {
    // arrange
    final SendMessageRequest request = SendMessageRequest.builder()
        .queueUrl("queueUrl")
        .messageBody("body")
        .messageAttributes(new HashMap<>())
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    executionAttributes.putAttribute(SPAN_EXECUTION_ATTRIBUTE, null);

    // act
    final SendMessageRequest newRequest =
        (SendMessageRequest) interceptor.modifyRequest(() -> request, executionAttributes);

    // assert
    assertThat(newRequest).isSameAs(request);
  }

  @Test
  public void afterExecutionWillFinishSpan() {
    // arrange
    final SendMessageRequest request = SendMessageRequest.builder()
        .queueUrl("queueUrl")
        .messageBody("body")
        .messageAttributes(new HashMap<>())
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    final Context.AfterExecution afterExecution = mockAfterExecutionSuccess(request, "messageId");

    // act
    interceptor.afterExecution(afterExecution, executionAttributes);

    // assert
    assertThat(spanHandler.spans()).isNotEmpty();
  }

  @Test
  public void afterExecutionOnSuccessWillAddResultingMessageIdToSpan() {
    // arrange
    final SendMessageRequest request = SendMessageRequest.builder()
        .queueUrl("queueUrl")
        .messageBody("body")
        .messageAttributes(new HashMap<>())
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    final Context.AfterExecution afterExecution =
        mockAfterExecutionSuccess(request, "returned-message-id");

    // act
    interceptor.afterExecution(afterExecution, executionAttributes);

    // assert
    assertThat(spanHandler.spans()).isNotEmpty();
    assertThat(spanHandler.get(0).tag("message.id")).isEqualTo("returned-message-id");
  }

  @Test
  public void afterExecutionOnFailureWillErrorOutTheSpan() {
    // arrange
    final SendMessageRequest request = SendMessageRequest.builder()
        .queueUrl("queueUrl")
        .messageBody("body")
        .messageAttributes(new HashMap<>())
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    final Context.AfterExecution afterExecution = mockAfterExecutionFailure(request, 400);

    // act
    interceptor.afterExecution(afterExecution, executionAttributes);

    // assert
    assertThat(spanHandler.spans()).isNotEmpty();
    assertThat(spanHandler.get(0).error()).hasMessage("Error placing message onto SQS queue");
    assertThat(spanHandler.get(0).tag("response.code")).isEqualTo("400");
  }

  @Test
  public void afterExecutionCustomSpanSuccessDecoratorWillRunAfterSuccess() {
    // arrange
    final SendMessageRequest request = SendMessageRequest.builder()
        .queueUrl("queueUrl")
        .messageBody("body")
        .messageAttributes(new HashMap<>())
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    final Context.AfterExecution afterExecution = mockAfterExecutionSuccess(request, "message-id");
    final SendMessageTracingExecutionInterceptor.SpanDecorator spanDecorator =
        new SendMessageTracingExecutionInterceptor.SpanDecorator() {
          @Override
          public void decorateMessageSpanOnSuccess(SendMessageRequest request,
              SendMessageResponse response, SdkHttpResponse sdkHttpResponse,
              Span span) {
            span.tag("test", "value");
          }
        };

    // act
    final SendMessageTracingExecutionInterceptor interceptor =
        new SendMessageTracingExecutionInterceptor(tracing, spanDecorator);
    interceptor.afterExecution(afterExecution, executionAttributes);

    // assert
    assertThat(spanHandler.spans()).isNotEmpty();
    assertThat(spanHandler.get(0).tag("test")).isEqualTo("value");
  }

  @Test
  public void afterExecutionCustomSpanFailureDecoratorWillRunAfterFailure() {
    // arrange
    final SendMessageRequest request = SendMessageRequest.builder()
        .queueUrl("queueUrl")
        .messageBody("body")
        .messageAttributes(new HashMap<>())
        .build();
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    final Context.AfterExecution afterExecution = mockAfterExecutionFailure(request, 500);
    final SendMessageTracingExecutionInterceptor.SpanDecorator spanDecorator =
        new SendMessageTracingExecutionInterceptor.SpanDecorator() {
          @Override
          public void decorateMessageSpanOnFailure(SendMessageRequest sendMessageRequest,
              SendMessageResponse response, SdkHttpResponse sdkHttpResponse,
              Span span) {
            span.tag("test", "value");

          }
        };

    // act
    final SendMessageTracingExecutionInterceptor interceptor =
        new SendMessageTracingExecutionInterceptor(tracing, spanDecorator);
    interceptor.afterExecution(afterExecution, executionAttributes);

    // assert
    assertThat(spanHandler.spans()).isNotEmpty();
    assertThat(spanHandler.get(0).tag("test")).isEqualTo("value");
    assertThat(spanHandler.get(0).error()).hasMessage("Error placing message onto SQS queue");
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
    executionAttributes.putAttribute(SPAN_EXECUTION_ATTRIBUTE, span);

    // act
    interceptor.afterExecution(afterExecution, executionAttributes);

    // assert
    assertThat(spanHandler.spans()).isEmpty();
  }

  @Test
  public void afterExecutionWillDoNothingIfNotSpanIsNotPresentInExecutionAttributes() {
    // arrange
    final SendMessageRequest request = SendMessageRequest.builder()
        .queueUrl("queueUrl")
        .messageBody("body")
        .messageAttributes(new HashMap<>())
        .build();
    final Context.AfterExecution afterExecution = mockAfterExecutionFailure(request, 400);
    final ExecutionAttributes executionAttributes = new ExecutionAttributes();
    interceptor.beforeExecution(() -> request, executionAttributes);
    executionAttributes.putAttribute(SPAN_EXECUTION_ATTRIBUTE, null);

    // act
    interceptor.afterExecution(afterExecution, executionAttributes);

    // assert
    assertThat(spanHandler.spans()).isEmpty();
  }

  private Context.AfterExecution mockAfterExecutionSuccess(final SdkRequest request,
      final String messageId) {
    final Context.AfterExecution afterExecution = mock(Context.AfterExecution.class);
    final SdkHttpResponse sdkHttpResponse = mock(SdkHttpResponse.class);
    when(afterExecution.httpResponse()).thenReturn(sdkHttpResponse);
    when(afterExecution.request()).thenReturn(request);
    when(sdkHttpResponse.isSuccessful()).thenReturn(true);
    when(afterExecution.response()).thenReturn(SendMessageResponse.builder()
        .messageId(messageId)
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