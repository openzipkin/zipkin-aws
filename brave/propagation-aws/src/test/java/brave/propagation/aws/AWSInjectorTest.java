/*
 * Copyright 2016-2023 The OpenZipkin Authors
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
package brave.propagation.aws;

import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import org.junit.Before;
import org.junit.Test;

import static brave.internal.codec.HexCodec.lowerHexToUnsignedLong;
import static brave.propagation.aws.AWSPropagation.AMZN_TRACE_ID_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AWSInjectorTest {
  private AWSInjector<Object> instance;
  private Propagation.Setter<Object, String> setterMock;

  // Regular trace ID and context.
  private final static String traceId =
      "Root=1-67891233-abcdef012345678912345678;Parent=463ac35c9f6413ad;Sampled=1";
  private final static TraceContext context = TraceContext.newBuilder()
      .traceIdHigh(lowerHexToUnsignedLong("67891233abcdef01"))
      .traceId(lowerHexToUnsignedLong("2345678912345678"))
      .spanId(lowerHexToUnsignedLong("463ac35c9f6413ad"))
      .sampled(true)
      .addExtra(AWSPropagation.NO_CUSTOM_FIELDS)
      .build();

  // Trace ID and context with the customFields set.
  private final static String traceIdCustomFields =
      "Root=1-67891233-abcdef012345678912345678;Parent=463ac35c9f6413ad;Sampled=1;CustomField=Foo-Bar";
  private final static TraceContext contextCustomFields = TraceContext.newBuilder()
      .traceIdHigh(lowerHexToUnsignedLong("67891233abcdef01"))
      .traceId(lowerHexToUnsignedLong("2345678912345678"))
      .spanId(lowerHexToUnsignedLong("463ac35c9f6413ad"))
      .sampled(true)
      .addExtra(new AWSPropagation.AmznTraceId(";CustomField=Foo-Bar"))
      .build();

  // Regular context where the extras are not set.
  private final static TraceContext contextNoExtras = TraceContext.newBuilder()
      .traceIdHigh(lowerHexToUnsignedLong("67891233abcdef01"))
      .traceId(lowerHexToUnsignedLong("2345678912345678"))
      .spanId(lowerHexToUnsignedLong("463ac35c9f6413ad"))
      .sampled(true)
      .build();

  @Before
  public void setUp() {
    // The setter is used to verify that the correct trace ID is generated.
    setterMock = (Propagation.Setter<Object, String>) mock(Propagation.Setter.class);
    // The AWSPropagator is unused, so passing null is fine for testing.
    // It's not possible to mock using Mockito, because it's a final class and mockito-inline is not used in the project.
    instance = new AWSInjector<>(null, setterMock);
  }

  @Test
  public void injectTraceContext_ExpectTraceId() {
    Object requestMock = mock(Object.class);

    // When injecting a trace context.
    instance.inject(context, requestMock);

    // Expect to see the corresponding trace ID set on the request.
    verify(setterMock, times(1)).put(requestMock, AMZN_TRACE_ID_NAME, traceId);
  }

  @Test
  public void injectTraceContextWithCustomFields_ExpectTraceIdWithCustomFields() {
    Object requestMock = mock(Object.class);

    // When injecting a trace context with custom fields.
    instance.inject(contextCustomFields, requestMock);

    // Expect to see the corresponding trace ID set on the request.
    verify(setterMock, times(1)).put(requestMock, AMZN_TRACE_ID_NAME, traceIdCustomFields);
  }

  @Test
  public void injectTraceContextWithNoExtras_ExpectTraceId() {
    Object requestMock = mock(Object.class);

    // When injecting a trace context with no extras set.
    instance.inject(contextNoExtras, requestMock);

    // Expect to see the corresponding trace ID set on the request.
    verify(setterMock, times(1)).put(requestMock, AMZN_TRACE_ID_NAME, traceId);
  }
}