/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package brave.instrumentation.awssdk.sqs;

import brave.Tracing;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;

public class SqsMessageTracing {
  public static SqsMessageTracing create(Tracing tracing) {
    return new SqsMessageTracing(tracing);
  }

  final ExecutionInterceptor executionInterceptor;

  private SqsMessageTracing(Tracing tracing) {
    this.executionInterceptor = new SendMessageTracingExecutionInterceptor(tracing);
  }

  public ExecutionInterceptor executionInterceptor() {
    return executionInterceptor;
  }
}
