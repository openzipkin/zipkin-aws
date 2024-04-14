/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package brave.instrumentation.aws;

import brave.Tracing;
import brave.handler.MutableSpan;
import brave.http.HttpTracing;
import brave.test.TestSpanHandler;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.DefaultRequest;
import com.amazonaws.handlers.HandlerAfterAttemptContext;
import com.amazonaws.handlers.HandlerContextKey;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TracingRequestHandlerTest {
  TestSpanHandler spans = new TestSpanHandler();
  Tracing tracing = Tracing.newBuilder().addSpanHandler(spans).build();
  TracingRequestHandler handler = new TracingRequestHandler(HttpTracing.create(tracing));

  @AfterEach void cleanup() {
    tracing.close();
  }

  @Test void handlesAmazonServiceExceptions() {
    brave.Span braveSpan = tracing.tracer().nextSpan();
    AmazonServiceException exception = new ProvisionedThroughputExceededException("test");
    exception.setRequestId("abcd");

    DefaultRequest request = new DefaultRequest("test");
    request.addHandlerContext(new HandlerContextKey<>(brave.Span.class.getCanonicalName()), braveSpan);
    HandlerAfterAttemptContext context = HandlerAfterAttemptContext.builder()
        .withRequest(request)
        .withException(exception)
        .build();

    handler.afterAttempt(context);
    assertThat(spans).hasSize(1);
    MutableSpan reportedSpan = spans.get(0);
    assertThat(reportedSpan.traceId()).isEqualToIgnoringCase(braveSpan.context().traceIdString());
    assertThat(reportedSpan.error()).isEqualTo(exception);
    assertThat(reportedSpan.tags().get("aws.request_id")).isEqualToIgnoringCase("abcd");
  }
}
