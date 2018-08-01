/*
 * Copyright 2016-2018 The OpenZipkin Authors
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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.util.StringUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
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

    AWSCredentialsProvider credentialsProvider;
    String appName;
    String streamName;
    String regionName;

    @Override
    public Builder storage(StorageComponent storageComponent) {
      delegate.storage(storageComponent);
      return this;
    }

    @Override
    public Builder metrics(CollectorMetrics metrics) {
      if (metrics == null) throw new NullPointerException("metrics == null");
      delegate.metrics(metrics.forTransport("kinesis"));
      return this;
    }

    @Override
    public Builder sampler(CollectorSampler collectorSampler) {
      delegate.sampler(collectorSampler);
      return this;
    }

    public Builder credentialsProvider(AWSCredentialsProvider credentialsProvider) {
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
  private final String appName;
  private final String streamName;
  private final AWSCredentialsProvider credentialsProvider;
  private final String regionName;

  private final Executor executor;
  private Worker worker;
  private IRecordProcessorFactory processor;

  KinesisCollector(Builder builder) {
    this.collector = builder.delegate.build();

    this.appName = builder.appName;
    this.streamName = builder.streamName;
    this.credentialsProvider = builder.credentialsProvider;
    this.regionName = builder.regionName;

    executor =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("KinesisCollector-" + streamName + "-%d")
                .build());
  }

  @Override
  public KinesisCollector start() {
    String workerId;
    try {
      workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
    } catch (UnknownHostException e) {
      workerId = UUID.randomUUID().toString();
    }

    KinesisClientLibConfiguration config =
        new KinesisClientLibConfiguration(appName, streamName, credentialsProvider, workerId);

    if (!StringUtils.isNullOrEmpty(regionName)) {
      config.withRegionName(regionName);
    }

    processor = new KinesisRecordProcessorFactory(collector);
    worker = new Worker.Builder().recordProcessorFactory(processor).config(config).build();

    executor.execute(worker);
    return this;
  }

  @Override
  public CheckResult check() {
    // TODO should check the stream exists
    return CheckResult.OK;
  }

  @Override
  public void close() {
    // The executor is a single thread that is tied to this worker. Once the worker shuts down
    // the executor will stop.
    worker.shutdown();
  }

  private static final class KinesisRecordProcessorFactory implements IRecordProcessorFactory {

    final Collector collector;

    KinesisRecordProcessorFactory(Collector collector) {
      this.collector = collector;
    }

    @Override
    public IRecordProcessor createProcessor() {
      return new KinesisSpanProcessor(collector);
    }
  }
}
