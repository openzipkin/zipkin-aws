package zipkin.collector.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import zipkin.Codec;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.collector.Collector;
import zipkin.storage.InMemoryStorage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;


public class KinesisSpanProcessorTest {

    private InMemoryStorage storage;
    private Collector collector;
    private KinesisSpanProcessor kinesisSpanProcessor;

    @Before
    public void setup() {
        storage = new InMemoryStorage();
        collector = Collector.builder(KinesisSpanProcessorTest.class).storage(storage).build();

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

        assertThat(storage.spanStore().getRawTraces().size()).isEqualTo(1);
    }

    @Test
    public void lotsOfRecordsCollected() {
        kinesisSpanProcessor.processRecords(createTestData(10000));

        assertThat(storage.spanStore().getRawTraces().size()).isEqualTo(10000);
    }

    private ProcessRecordsInput createTestData(long count) {
        List<Record> records = new ArrayList<>();

        Span[] spans = new Random().longs(count).mapToObj(TestObjects::span).toArray(Span[]::new);

        Arrays.stream(spans)
                .map(s -> ByteBuffer.wrap(Codec.THRIFT.writeSpan(s)))
                .map(b -> new Record().withData(b))
                .forEach(records::add);

        return new ProcessRecordsInput().withRecords(records);
    }

}
