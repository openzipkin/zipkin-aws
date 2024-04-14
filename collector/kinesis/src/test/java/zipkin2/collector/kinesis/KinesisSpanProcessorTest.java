/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.collector.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.Record;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.Span;
import zipkin2.TestObjects;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.collector.Collector;
import zipkin2.collector.InMemoryCollectorMetrics;
import zipkin2.storage.InMemoryStorage;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/** We can't integration test the KinesisCollector without a local version of the service */
class KinesisSpanProcessorTest {
  List<Span> spans =
      asList( // No unicode or data that doesn't translate between json formats
          TestObjects.LOTS_OF_SPANS[0], TestObjects.LOTS_OF_SPANS[1], TestObjects.LOTS_OF_SPANS[2]);

  private InMemoryStorage storage;
  private final InMemoryCollectorMetrics metrics = new InMemoryCollectorMetrics();

  private Collector collector;
  private KinesisSpanProcessor kinesisSpanProcessor;

  @BeforeEach void setup() {
    storage = InMemoryStorage.newBuilder().build();
    collector = Collector.newBuilder(KinesisSpanProcessorTest.class)
        .storage(storage)
        .metrics(metrics)
        .build();

    kinesisSpanProcessor = new KinesisSpanProcessor(collector, metrics);
  }

  @AfterEach void teardown() {
    kinesisSpanProcessor = null;
    collector = null;
    storage = null;
  }

  @Test void oneRecordCollected() {
    kinesisSpanProcessor.processRecords(createTestData(1));

    assertThat(storage.spanStore().getTraces().size()).isEqualTo(1);
  }

  /** Ensures list encoding works: a version 2 json list of spans */
  @Test void messageWithMultipleSpans_json2() {
    messageWithMultipleSpans(SpanBytesEncoder.JSON_V2);
  }

  /** Ensures list encoding works: proto3 ListOfSpans */
  @Test void messageWithMultipleSpans_proto3() {
    messageWithMultipleSpans(SpanBytesEncoder.PROTO3);
  }

  void messageWithMultipleSpans(SpanBytesEncoder encoder) {
    byte[] message = encoder.encodeList(spans);

    List<Record> records = Collections.singletonList(new Record().withData(ByteBuffer.wrap(message)));
    kinesisSpanProcessor.processRecords(new ProcessRecordsInput().withRecords(records));

    assertThat(storage.spanStore().getTraces().size()).isEqualTo(spans.size());
  }

  @Test void lotsOfRecordsCollected() {
    kinesisSpanProcessor.processRecords(createTestData(10000));

    assertThat(storage.spanStore().getTraces().size()).isEqualTo(10000);
  }

  @Test void collectorFailsWhenRecordEncodedAsSingleSpan() {
    Span span = TestObjects.LOTS_OF_SPANS[0];
    byte[] encodedSpan = SpanBytesEncoder.THRIFT.encode(span);
    Record kinesisRecord = new Record().withData(ByteBuffer.wrap(encodedSpan));
    ProcessRecordsInput kinesisInput =
        new ProcessRecordsInput().withRecords(Collections.singletonList(kinesisRecord));

    kinesisSpanProcessor.processRecords(kinesisInput);

    assertThat(storage.spanStore().getTraces().size()).isEqualTo(0);

    assertThat(metrics.messages()).isEqualTo(1);
    assertThat(metrics.messagesDropped()).isEqualTo(1);
    assertThat(metrics.bytes()).isEqualTo(encodedSpan.length);
  }

  private ProcessRecordsInput createTestData(int count) {
    List<Record> records = new ArrayList<>();

    Span[] spans = Arrays.copyOfRange(TestObjects.LOTS_OF_SPANS, 0, count);

    Arrays.stream(spans)
        .map(s -> ByteBuffer.wrap(SpanBytesEncoder.THRIFT.encodeList(Collections.singletonList(s))))
        .map(b -> new Record().withData(b))
        .forEach(records::add);

    return new ProcessRecordsInput().withRecords(records);
  }
}
