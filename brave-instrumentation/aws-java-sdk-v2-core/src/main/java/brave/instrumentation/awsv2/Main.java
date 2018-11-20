package brave.instrumentation.awsv2;

import brave.Tracing;
import brave.http.HttpTracing;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class Main {
  public static final void main(String[] args) throws InterruptedException {
    Tracing tracing = Tracing.newBuilder().build();
    TracingExecutionInterceptor interceptor = new TracingExecutionInterceptor(HttpTracing.create(tracing));

    SdkHttpClient client = ApacheHttpClient.builder().build();
    S3Client s3 = S3Client.builder()
        .httpClient(client)
        .region(Region.US_EAST_1)
        .overrideConfiguration(ClientOverrideConfiguration.builder()
            .addExecutionInterceptor(interceptor)
            .build())
        .build();

    s3.listBuckets();
    Thread.sleep(1000);
  }
}
