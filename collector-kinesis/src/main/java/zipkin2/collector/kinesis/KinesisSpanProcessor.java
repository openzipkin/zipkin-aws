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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import zipkin2.Callback;
import zipkin2.Span;
import zipkin2.SpanBytesDecoderDetector;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.collector.Collector;
import zipkin2.collector.CollectorMetrics;

final class KinesisSpanProcessor implements ShardRecordProcessor {
  static final Logger LOGGER = Logger.getLogger(KinesisCollector.class.getName());
  static final Callback<Void> NOOP = new Callback<Void>() {
    @Override public void onSuccess(Void value) {
    }

    @Override public void onError(Throwable t) {
    }
  };

  final Collector collector;
  final CollectorMetrics metrics;

  KinesisSpanProcessor(Collector collector, CollectorMetrics metrics) {
    this.collector = collector;
    this.metrics = metrics;
  }

  @Override public void initialize(InitializationInput initializationInput) {
  }

  @Override public void processRecords(ProcessRecordsInput processRecordsInput) {
    boolean shouldLog = LOGGER.isLoggable(Level.FINE);

    for (KinesisClientRecord record : processRecordsInput.records()) {
      ByteBuffer nioBuffer = record.data();
      metrics.incrementMessages();
      metrics.incrementBytes(record.data().remaining());

      SpanBytesDecoder decoder;
      try {
        decoder = (SpanBytesDecoder) SpanBytesDecoderDetector.decoderForListMessage(nioBuffer);
      } catch (IllegalArgumentException e) {
        if (shouldLog) LOGGER.log(Level.FINE, "error detecting encoding", e);
        metrics.incrementMessagesDropped();
        continue;
      }

      List<Span> spans = new ArrayList<>();
      if (!decoder.decodeList(nioBuffer, spans)) {
        if (shouldLog) LOGGER.log(Level.FINE, "Empty " + decoder.name() + " message");
        continue;
      }

      // UnzippingBytesRequestConverter handles incrementing message and bytes
      collector.accept(spans, NOOP); // async
    }
  }

  @Override public void leaseLost(LeaseLostInput leaseLostInput) {
  }

  @Override public void shardEnded(ShardEndedInput shardEndedInput) {
  }

  @Override public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
  }
}
