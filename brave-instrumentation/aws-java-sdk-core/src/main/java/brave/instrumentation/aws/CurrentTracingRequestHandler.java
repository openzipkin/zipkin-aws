package brave.instrumentation.aws;

import brave.Tracer;
import brave.Tracing;

public class CurrentTracingRequestHandler extends TracingRequestHandler {
  public CurrentTracingRequestHandler() {
    super();
  }

  @Override protected Tracer tracer() {
    return Tracing.currentTracer();
  }
}
