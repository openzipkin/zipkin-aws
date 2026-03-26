/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.collector.kinesis;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
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

    AwsCredentialsProvider credentialsProvider;
    String appName;
    String streamName;
    String regionName = "us-east-1";

    @Override
    public Builder storage(StorageComponent storageComponent) {
      delegate.storage(storageComponent);
      return this;
    }

    @Override
    public Builder metrics(CollectorMetrics metrics) {
      if (metrics == null) throw new NullPointerException("metrics == null");
      delegate.metrics(this.metrics = metrics.forTransport("kinesis"));
      return this;
    }

    @Override
    public Builder sampler(CollectorSampler collectorSampler) {
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

    @Override
    public KinesisCollector build() {
      return new KinesisCollector(this);
    }

    Builder() {}
  }

  private final Collector collector;
  private final CollectorMetrics metrics;
  private final String appName;
  private final String streamName;
  private final AwsCredentialsProvider credentialsProvider;
  private final String regionName;

  private final Executor executor;
  private Scheduler scheduler;
  private KinesisAsyncClient kinesisClient;
  private DynamoDbAsyncClient dynamoClient;
  private CloudWatchAsyncClient cloudWatchClient;

  KinesisCollector(Builder builder) {
    this.collector = builder.delegate.build();
    this.metrics = builder.metrics;
    this.appName = builder.appName;
    this.streamName = builder.streamName;
    this.credentialsProvider = builder.credentialsProvider;
    this.regionName = builder.regionName;

    executor = Executors.newSingleThreadExecutor(r -> {
      Thread thread = new Thread(r);
      thread.setName("KinesisCollector-" + streamName);
      thread.setDaemon(true);
      return thread;
    });
  }

  @Override
  public KinesisCollector start() {
    String workerId;
    try {
      workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
    } catch (UnknownHostException e) {
      workerId = UUID.randomUUID().toString();
    }

    Region region = Region.of(regionName);

    kinesisClient = KinesisAsyncClient.builder()
        .credentialsProvider(credentialsProvider)
        .region(region)
        .build();

    dynamoClient = DynamoDbAsyncClient.builder()
        .credentialsProvider(credentialsProvider)
        .region(region)
        .build();

    cloudWatchClient = CloudWatchAsyncClient.builder()
        .credentialsProvider(credentialsProvider)
        .region(region)
        .build();

    ShardRecordProcessorFactory processorFactory =
        () -> new KinesisSpanProcessor(collector, metrics);

    ConfigsBuilder configsBuilder = new ConfigsBuilder(
        streamName, appName, kinesisClient, dynamoClient, cloudWatchClient,
        workerId, processorFactory);

    scheduler = new Scheduler(
        configsBuilder.checkpointConfig(),
        configsBuilder.coordinatorConfig(),
        configsBuilder.leaseManagementConfig(),
        configsBuilder.lifecycleConfig(),
        configsBuilder.metricsConfig(),
        configsBuilder.processorConfig(),
        configsBuilder.retrievalConfig());

    executor.execute(scheduler);
    return this;
  }

  @Override
  public CheckResult check() {
    // TODO should check the stream exists
    return CheckResult.OK;
  }

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.shutdown();
    }
    if (kinesisClient != null) kinesisClient.close();
    if (dynamoClient != null) dynamoClient.close();
    if (cloudWatchClient != null) cloudWatchClient.close();
  }
}
