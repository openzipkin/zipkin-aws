/**
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
package zipkin.collector.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.Record;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import zipkin.Codec;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.collector.Collector;
import zipkin.collector.InMemoryCollectorMetrics;
import zipkin.internal.ApplyTimestampAndDuration;
import zipkin.internal.V2SpanConverter;
import zipkin.internal.V2StorageComponent;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.InMemoryStorage;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * We can't integration test the KinesisCollector without a local version of the service
 */
public class KinesisSpanProcessorTest {
  List<Span> spans = asList( // No unicode or data that doesn't translate between json formats
      ApplyTimestampAndDuration.apply(TestObjects.LOTS_OF_SPANS[0]),
      ApplyTimestampAndDuration.apply(TestObjects.LOTS_OF_SPANS[1]),
      ApplyTimestampAndDuration.apply(TestObjects.LOTS_OF_SPANS[2])
  );

  private InMemoryStorage storage;
  private InMemoryCollectorMetrics metrics = new InMemoryCollectorMetrics();

  private Collector collector;
  private KinesisSpanProcessor kinesisSpanProcessor;

  @Before
  public void setup() {
    storage = InMemoryStorage.newBuilder().build();
    collector = Collector.builder(KinesisSpanProcessorTest.class)
        .storage(V2StorageComponent.create(storage))
        .metrics(metrics)
        .build();

    kinesisSpanProcessor = new KinesisSpanProcessor(collector);
  }

  @After
  public void teardown() {
    kinesisSpanProcessor = null;
    collector = null;
    storage = null;
  }

  @Test
  public void oneRecordCollected() {
    kinesisSpanProcessor.processRecords(createTestData(1));

    assertThat(storage.spanStore().getTraces().size()).isEqualTo(1);
  }

  /** Ensures list encoding works: a version 2 json list of spans */
  @Test public void messageWithMultipleSpans_json2() {
    messageWithMultipleSpans(SpanBytesEncoder.JSON_V2);
  }

  /** Ensures list encoding works: proto3 ListOfSpans */
  @Test public void messageWithMultipleSpans_proto3() {
    messageWithMultipleSpans(SpanBytesEncoder.PROTO3);
  }

  void messageWithMultipleSpans(SpanBytesEncoder encoder) {
    byte[] message = encoder.encodeList(V2SpanConverter.fromSpans(spans));

    List<Record> records = Arrays.asList(new Record().withData(ByteBuffer.wrap(message)));
    kinesisSpanProcessor.processRecords(new ProcessRecordsInput().withRecords(records));

    assertThat(storage.spanStore().getTraces().size()).isEqualTo(spans.size());
  }

  @Test
  public void lotsOfRecordsCollected() {
    kinesisSpanProcessor.processRecords(createTestData(10000));

    assertThat(storage.spanStore().getTraces().size()).isEqualTo(10000);
  }

  @Test
  public void collectorFailsWhenRecordEncodedAsSingleSpan() {
    Span span = TestObjects.LOTS_OF_SPANS[0];
    byte[] encodedSpan = Codec.THRIFT.writeSpan(span);
    Record kinesisRecord = new Record().withData(ByteBuffer.wrap(encodedSpan));
    ProcessRecordsInput kinesisInput = new ProcessRecordsInput().withRecords(Collections.singletonList(kinesisRecord));

    kinesisSpanProcessor.processRecords(kinesisInput);

    assertThat(storage.spanStore().getTraces().size()).isEqualTo(0);

    assertThat(metrics.messagesDropped()).isEqualTo(1);
    assertThat(metrics.bytes()).isEqualTo(encodedSpan.length);
  }

  private ProcessRecordsInput createTestData(int count) {
    List<Record> records = new ArrayList<>();

    Span[] spans = Arrays.copyOfRange(TestObjects.LOTS_OF_SPANS, 0, count);

    Arrays.stream(spans)
        .map(s -> ByteBuffer.wrap(Codec.THRIFT.writeSpans(Collections.singletonList(s))))
        .map(b -> new Record().withData(b))
        .forEach(records::add);

    return new ProcessRecordsInput().withRecords(records);
  }

}
