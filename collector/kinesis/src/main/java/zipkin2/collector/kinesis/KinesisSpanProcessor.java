/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.collector.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import zipkin2.Callback;
import zipkin2.collector.Collector;
import zipkin2.collector.CollectorMetrics;

final class KinesisSpanProcessor implements IRecordProcessor {
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
    for (Record record : processRecordsInput.getRecords()) {
      byte[] serialized = record.getData().array();
      metrics.incrementMessages();
      metrics.incrementBytes(serialized.length);
      collector.acceptSpans(serialized, NOOP); // async
    }
  }

  @Override
  public void shutdown(ShutdownInput shutdownInput) {
  }
}
