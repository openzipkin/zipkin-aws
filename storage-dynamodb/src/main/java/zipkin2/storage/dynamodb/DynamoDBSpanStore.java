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
package zipkin2.storage.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import java.util.List;
import java.util.concurrent.Executor;
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
  private final Executor executor;
  private final String spansTableName;
  private final String serviceSpanNamesTableName;

  DynamoDBSpanStore(DynamoDBStorage.Builder builder) {
    this.strictTraceId = builder.strictTraceId;
    this.searchEnabled = builder.searchEnabled;
    this.dynamoDB = builder.dynamoDB;
    this.executor = builder.executor;

    this.spansTableName = builder.tablePrefix + SPANS_TABLE_BASE_NAME;
    this.serviceSpanNamesTableName = builder.tablePrefix + SERVICE_SPAN_NAMES_TABLE_BASE_NAME;
  }

  @Override public Call<List<List<Span>>> getTraces(QueryRequest queryRequest) {
    if (!searchEnabled) {
      return Call.emptyList();
    }
    return new GetTracesForQueryCall(executor, strictTraceId, dynamoDB, spansTableName,
        queryRequest);
  }

  @Override public Call<List<Span>> getTrace(String s) {
    if (!searchEnabled) {
      return Call.emptyList();
    }
    return new GetTraceByIdCall(executor, dynamoDB, spansTableName, strictTraceId, s);
  }

  @Override public Call<List<String>> getServiceNames() {
    if (!searchEnabled) {
      return Call.emptyList();
    }
    return new GetServiceNamesCall(executor, dynamoDB, serviceSpanNamesTableName);
  }

  @Override public Call<List<String>> getSpanNames(String s) {
    if (!searchEnabled) {
      return Call.emptyList();
    }
    return new GetSpanNamesCall(executor, dynamoDB, serviceSpanNamesTableName,
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
    return new GetDependenciesCall(executor, dynamoDB, endTs, lookback);
  }
}
