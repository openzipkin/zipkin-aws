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
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.util.Map;

import static brave.Span.Kind.PRODUCER;

final class SendMessageTracingRequestHandler extends RequestHandler2 {

  static final RemoteSetter<Map<String, MessageAttributeValue>> SETTER =
      new RemoteSetter<Map<String, MessageAttributeValue>>() {
        @Override public Span.Kind spanKind() {
          return PRODUCER;
        }

        @Override public void put(Map<String, MessageAttributeValue> carrier, String key, String value) {
          carrier.put(key,
              new MessageAttributeValue().withDataType("String").withStringValue(value));
        }
      };

  static final RemoteGetter<Map<String, MessageAttributeValue>> GETTER =
      new RemoteGetter<Map<String, MessageAttributeValue>>() {
        @Override public Span.Kind spanKind() {
          return PRODUCER;
        }

        @Override public String get(Map<String, MessageAttributeValue> carrier, String key) {
          return carrier.containsKey(key) ? carrier.get(key).getStringValue() : null;
        }
      };

  final Tracing tracing;
  final Tracer tracer;
  final CurrentTraceContext currentTraceContext;
  final Injector<Map<String, MessageAttributeValue>> injector;
  final Extractor<Map<String, MessageAttributeValue>> extractor;

  SendMessageTracingRequestHandler(Tracing tracing) {
    this.tracing = tracing;
    this.tracer = tracing.tracer();
    this.currentTraceContext = tracing.currentTraceContext();
    this.injector = tracing.propagation().injector(SETTER);
    this.extractor = tracing.propagation().extractor(GETTER);
  }

  @Override
  public AmazonWebServiceRequest beforeExecution(AmazonWebServiceRequest request) {
    if (request instanceof SendMessageRequest) {
      handleSendMessageRequest((SendMessageRequest) request);
    } else if (request instanceof SendMessageBatchRequest) {
      handleSendMessageBatchRequest((SendMessageBatchRequest) request);
    }
    return request;
  }

  private void handleSendMessageRequest(SendMessageRequest request) {
    injectPerMessage(request.getQueueUrl(), request.getMessageAttributes());
  }

  private void handleSendMessageBatchRequest(SendMessageBatchRequest request) {
    TraceContext maybeParent = currentTraceContext.get();

    Span span;
    if (maybeParent == null) {
      span = tracer.nextSpan();
    } else {
      // If we have a span in scope assume headers were cleared before
      span = tracer.newChild(maybeParent);
    }

    span.name("publish-batch").remoteServiceName("amazon-sqs").start();
    try (SpanInScope scope = tracer.withSpanInScope(span)) {
      for (SendMessageBatchRequestEntry entry : request.getEntries()) {
        injectPerMessage(request.getQueueUrl(), entry.getMessageAttributes());
      }
    } finally {
      span.finish();
    }
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
