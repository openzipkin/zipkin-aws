package zipkin2.storage.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import zipkin2.internal.HexCodec;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;

/**
 * We use the following tables: zipkin-spans, zipkin-traces, zipkin-service-names,
 * zipkin-span-names, zipkin-service-span-names
 *
 * zipkin-spans: trace_id span_id_composite (timestamp << 64 + (unsigned) span_id) span_id span
 * tag_value_pairs[] tags[] timestamp duration ttl
 *
 * zipkin-traces: trace_id tag_value_pairs[] tags[] earliest_timestamp latest_timestamp duration
 * ttl
 *
 * zipkin-names: name_type service_name ttl
 *
 * zipkin-span-names: span_name ttl
 *
 * zipkin-service-span-names: service_name span_name ttl
 *
 * In the event a query contains either span_name or service_name, the query will run against the
 * zipkin-spans table, otherwise it will be a scan against zipkin-traces
 */
public final class DynamoDBStorage extends StorageComponent {
  private DynamoDBSpanStore dynamoDBSpanStore;
  private DynamoDBSpanConsumer dynamoDBSpanConsumer;
  private DynamoDBAutocompleteTags dynamoDBAutocompleteTags;

  DynamoDBStorage(DynamoDBStorage.Builder builder) {
    dynamoDBSpanStore = new DynamoDBSpanStore(builder);
    dynamoDBSpanConsumer = new DynamoDBSpanConsumer(builder);
    dynamoDBAutocompleteTags = new DynamoDBAutocompleteTags(builder);
  }

  static String timestampId(long timestamp, String spanId) {
    return BigInteger.valueOf(timestamp)
        .shiftLeft(Long.SIZE)
        .add(BigInteger.valueOf(HexCodec.lowerHexToUnsignedLong(spanId)))
        .toString();
  }

  @Override public SpanStore spanStore() {
    return dynamoDBSpanStore;
  }

  @Override public SpanConsumer spanConsumer() {
    return dynamoDBSpanConsumer;
  }

  @Override public AutocompleteTags autocompleteTags() {
    return dynamoDBAutocompleteTags;
  }

  public static final class Builder extends StorageComponent.Builder {
    boolean strictTraceId = true;
    boolean searchEnabled = true;
    List<String> autocompleteKeys = Collections.emptyList();
    AmazonDynamoDBAsync dynamoDB;
    String tablePrefix = "zipkin-";
    ExecutorService executorService = Executors.newCachedThreadPool();

    /** {@inheritDoc} */
    @Override
    public DynamoDBStorage.Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    @Override
    public DynamoDBStorage.Builder searchEnabled(boolean searchEnabled) {
      this.searchEnabled = searchEnabled;
      return this;
    }

    @Override
    public DynamoDBStorage.Builder autocompleteKeys(List<String> autocompleteKeys) {
      if (autocompleteKeys == null) throw new NullPointerException("autocompleteKeys == null");
      this.autocompleteKeys = autocompleteKeys;
      return this;
    }

    public DynamoDBStorage.Builder dynamoDB(AmazonDynamoDBAsync dynamoDB) {
      this.dynamoDB = dynamoDB;
      return this;
    }

    public DynamoDBStorage.Builder tablePrefix(String tablePrefix) {
      this.tablePrefix = tablePrefix;
      return this;
    }

    public DynamoDBStorage.Builder executorService(ExecutorService executorService) {
      this.executorService = executorService;
      return this;
    }

    @Override public StorageComponent build() {
      return new DynamoDBStorage(this);
    }
  }
}
