package zipkin2.storage.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import java.util.List;
import java.util.concurrent.ExecutorService;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;

import static zipkin2.storage.dynamodb.DynamoDBConstants.SERVICE_SPAN_NAMES_TABLE_BASE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.SPANS_TABLE_BASE_NAME;

final class DynamoDBSpanStore implements SpanStore {

  private final boolean strictTraceId;
  private final boolean searchEnabled;
  private final AmazonDynamoDBAsync dynamoDB;
  private final ExecutorService executorService;
  private final String spansTableName;
  private final String serviceSpanNamesTableName;

  DynamoDBSpanStore(DynamoDBStorage.Builder builder) {
    this.strictTraceId = builder.strictTraceId;
    this.searchEnabled = builder.searchEnabled;
    this.dynamoDB = builder.dynamoDB;
    this.executorService = builder.executorService;

    this.spansTableName = builder.tablePrefix + SPANS_TABLE_BASE_NAME;
    this.serviceSpanNamesTableName = builder.tablePrefix + SERVICE_SPAN_NAMES_TABLE_BASE_NAME;
  }

  @Override public Call<List<List<Span>>> getTraces(QueryRequest queryRequest) {
    if (!searchEnabled) {
      return Call.emptyList();
    }
    return new GetTracesForQueryCall(executorService, strictTraceId, dynamoDB, spansTableName,
        queryRequest);
  }

  @Override public Call<List<Span>> getTrace(String s) {
    if (!searchEnabled) {
      return Call.emptyList();
    }
    return new GetTraceByIdCall(executorService, dynamoDB, spansTableName, strictTraceId, s);
  }

  @Override public Call<List<String>> getServiceNames() {
    if (!searchEnabled) {
      return Call.emptyList();
    }
    return new GetServiceNamesCall(executorService, dynamoDB, serviceSpanNamesTableName);
  }

  @Override public Call<List<String>> getSpanNames(String s) {
    if (!searchEnabled) {
      return Call.emptyList();
    }
    return new GetSpanNamesCall(executorService, dynamoDB, serviceSpanNamesTableName,
        s.toLowerCase());
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    if (!searchEnabled) {
      return Call.emptyList();
    }
    if (endTs <= 0) {
      throw new IllegalArgumentException("endTs <= 0");
    } else if (lookback <= 0) {
      throw new IllegalArgumentException("lookback <= 0");
    }
    return new GetDependenciesCall(executorService, dynamoDB, endTs, lookback);
  }
}
