/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.collector.kinesis;

import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import zipkin2.Callback;
import zipkin2.collector.Collector;
import zipkin2.collector.CollectorMetrics;

final class KinesisSpanProcessor implements ShardRecordProcessor {
  static final Callback<Void> NOOP = new Callback<>() {
    @Override
    public void onSuccess(Void value) {
    }

    @Override
    public void onError(Throwable t) {
    }
  };

  final Collector collector;
  final CollectorMetrics metrics;

  KinesisSpanProcessor(Collector collector, CollectorMetrics metrics) {
    this.collector = collector;
    this.metrics = metrics;
  }

  @Override
  public void initialize(InitializationInput initializationInput) {
  }

  @Override
  public void processRecords(ProcessRecordsInput processRecordsInput) {
    for (KinesisClientRecord record : processRecordsInput.records()) {
      byte[] serialized = new byte[record.data().remaining()];
      record.data().get(serialized);
      metrics.incrementMessages();
      metrics.incrementBytes(serialized.length);
      collector.acceptSpans(serialized, NOOP); // async
    }
  }

  @Override
  public void leaseLost(LeaseLostInput leaseLostInput) {
  }

  @Override
  public void shardEnded(ShardEndedInput shardEndedInput) {
  }

  @Override
  public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
  }
}
