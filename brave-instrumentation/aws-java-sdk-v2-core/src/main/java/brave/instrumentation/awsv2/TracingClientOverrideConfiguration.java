package brave.instrumentation.awsv2;

import brave.http.HttpTracing;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;

public class TracingClientOverrideConfiguration {
  public static TracingClientOverrideConfiguration create(HttpTracing httpTracing) {
    return new TracingClientOverrideConfiguration(httpTracing);
  }

  final HttpTracing httpTracing;

  TracingClientOverrideConfiguration(HttpTracing httpTracing) {
    if (httpTracing == null) throw new NullPointerException("httpTracing == null");
    this.httpTracing = httpTracing;
  }

  public ClientOverrideConfiguration build(ClientOverrideConfiguration.Builder builder) {
    if (builder == null) throw new NullPointerException("builder == null");
    builder.addExecutionInterceptor(new TracingExecutionInterceptor(httpTracing));

    return builder.build();
  }
}
