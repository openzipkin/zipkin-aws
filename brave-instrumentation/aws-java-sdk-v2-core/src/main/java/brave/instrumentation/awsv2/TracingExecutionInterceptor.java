package brave.instrumentation.awsv2;

import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;

public class TracingExecutionInterceptor implements ExecutionInterceptor {
  @Override public void beforeExecution(Context.BeforeExecution context,
      ExecutionAttributes executionAttributes) {
  }

  @Override public void beforeUnmarshalling(Context.BeforeUnmarshalling context,
      ExecutionAttributes executionAttributes) {
  }

  @Override public void afterExecution(Context.AfterExecution context,
      ExecutionAttributes executionAttributes) {
  }

  @Override public void onExecutionFailure(Context.FailedExecution context,
      ExecutionAttributes executionAttributes) {
  }
}
