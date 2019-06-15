/*
 * Copyright 2016-2019 The OpenZipkin Authors
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
package zipkin2.collector.kinesis;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.polling.PollingConfig;
import zipkin2.CheckResult;
import zipkin2.collector.Collector;
import zipkin2.collector.CollectorComponent;
import zipkin2.collector.CollectorMetrics;
import zipkin2.collector.CollectorSampler;
import zipkin2.storage.StorageComponent;

public final class KinesisCollector extends CollectorComponent {
  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends CollectorComponent.Builder {

    Collector.Builder delegate = Collector.newBuilder(KinesisCollector.class);
    CollectorMetrics metrics = CollectorMetrics.NOOP_METRICS;

    AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
    String appName;
    String streamName;
    String regionName;
    URI kinesisEndpointOverride, dynamoEndpointOverride, cloudWatchEndpointOverride;

    @Override public Builder storage(StorageComponent storageComponent) {
      delegate.storage(storageComponent);
      return this;
    }

    @Override public Builder metrics(CollectorMetrics metrics) {
      if (metrics == null) throw new NullPointerException("metrics == null");
      delegate.metrics(this.metrics = metrics.forTransport("kinesis"));
      return this;
    }

    @Override public Builder sampler(CollectorSampler collectorSampler) {
      delegate.sampler(collectorSampler);
      return this;
    }

    public Builder credentialsProvider(AwsCredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    public Builder appName(String appName) {
      this.appName = appName;
      return this;
    }

    public Builder streamName(String streamName) {
      this.streamName = streamName;
      return this;
    }

    public Builder regionName(String regionName) {
      this.regionName = regionName;
      return this;
    }

    @Override public KinesisCollector build() {
      return new KinesisCollector(this);
    }

    Builder() {
    }
  }

  final Collector collector;
  final CollectorMetrics metrics;
  final String appName;
  final String streamName;
  final AwsCredentialsProvider credentialsProvider;
  final String regionName;
  final URI kinesisEndpointOverride, dynamoEndpointOverride, cloudWatchEndpointOverride;

  final Executor executor;
  final SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder().build();
  private Scheduler scheduler;
  private ShardRecordProcessorFactory processor;

  KinesisCollector(Builder builder) {
    collector = builder.delegate.build();
    metrics = builder.metrics;
    appName = builder.appName;
    streamName = builder.streamName;
    credentialsProvider = builder.credentialsProvider;
    regionName = builder.regionName;
    kinesisEndpointOverride = builder.kinesisEndpointOverride;
    dynamoEndpointOverride = builder.dynamoEndpointOverride;
    cloudWatchEndpointOverride = builder.cloudWatchEndpointOverride;
    executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("KinesisCollector-" + streamName + "-%d")
        .build());
  }

  @Override public KinesisCollector start() {
    String workerId;
    try {
      workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
    } catch (UnknownHostException e) {
      workerId = UUID.randomUUID().toString();
    }

    KinesisAsyncClient kinesisClient = maybeOverrideEndpoint(KinesisAsyncClient.builder()
        .httpClient(httpClient), kinesisEndpointOverride).build();

    DynamoDbAsyncClient dynamoClient = maybeOverrideEndpoint(DynamoDbAsyncClient.builder()
        .httpClient(httpClient), dynamoEndpointOverride).build();

    CloudWatchAsyncClient cloudWatchClient = maybeOverrideEndpoint(CloudWatchAsyncClient.builder()
        .httpClient(httpClient), cloudWatchEndpointOverride).build();

    processor = () -> new KinesisSpanProcessor(collector, metrics);

    ConfigsBuilder configsBuilder = new ConfigsBuilder(
        streamName, appName, kinesisClient, dynamoClient, cloudWatchClient, workerId, processor);

    scheduler = new Scheduler(
        configsBuilder.checkpointConfig(),
        configsBuilder.coordinatorConfig(),
        configsBuilder.leaseManagementConfig(),
        configsBuilder.lifecycleConfig(),
        configsBuilder.metricsConfig(),
        configsBuilder.processorConfig(),
        configsBuilder.retrievalConfig()
            .retrievalSpecificConfig(new PollingConfig(streamName, kinesisClient))
    );

    executor.execute(scheduler);
    return this;
  }

  <B extends AwsClientBuilder<B, C>, C> B maybeOverrideEndpoint(B builder, URI nullableEndpoint) {
    builder.credentialsProvider(credentialsProvider);
    if (regionName != null) builder.region(Region.of(regionName));
    return nullableEndpoint != null ? builder.endpointOverride(nullableEndpoint) : builder;
  }

  @Override public CheckResult check() {
    // TODO should check the stream exists
    return CheckResult.OK;
  }

  @Override public void close() {
    // The executor is a single thread that is tied to this scheduler. Once the scheduler shuts down
    // the executor will stop.
    executor.execute(scheduler::shutdown);
    httpClient.close();
  }
}
