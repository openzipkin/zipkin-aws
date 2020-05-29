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
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TracingRequestHandlerTest {
  TestSpanHandler spans = new TestSpanHandler();
  Tracing tracing = Tracing.newBuilder().addSpanHandler(spans).build();
  TracingRequestHandler handler = new TracingRequestHandler(HttpTracing.create(tracing));

  @After
  public void cleanup() {
    tracing.close();
  }

  @Test
  public void handlesAmazonServiceExceptions() {
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
