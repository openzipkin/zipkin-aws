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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;
import zipkin2.storage.dynamodb.internal.CollectIntoList;

import static zipkin2.storage.dynamodb.DynamoDBConstants.SEARCH_TABLE_BASE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.SPANS_TABLE_BASE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.SERVICE_SPAN_ENTITY_TYPE;

final class DynamoDBSpanStore implements SpanStore {

  final boolean strictTraceId;
  final boolean searchEnabled;
  final AmazonDynamoDBAsync dynamoDB;
  final String spansTableName;
  final String searchTableName;

  DynamoDBSpanStore(DynamoDBStorage.Builder builder) {
    this.strictTraceId = builder.strictTraceId;
    this.searchEnabled = builder.searchEnabled;
    this.dynamoDB = builder.client;
    this.spansTableName = builder.tablePrefix + SPANS_TABLE_BASE_NAME;
    this.searchTableName = builder.tablePrefix + SEARCH_TABLE_BASE_NAME;
  }

  @Override public Call<List<List<Span>>> getTraces(QueryRequest queryRequest) {
    if (!searchEnabled) return Call.emptyList();
    return GetTraceIdsCall.create(strictTraceId, dynamoDB, spansTableName, queryRequest)
        .flatMap(traceIds -> {
          if (traceIds.isEmpty()) return Call.emptyList();
          if (traceIds.size() == 1) {
            return getTrace(traceIds.iterator().next()).map(Collections::singletonList);
          }
          return new CollectIntoList<>(
              traceIds.stream().map(this::getTrace).collect(Collectors.toList()));
        });
  }

  @Override public Call<List<Span>> getTrace(String traceId) {
    traceId = Span.normalizeTraceId(traceId);
    return GetTraceByIdCall.create(dynamoDB, spansTableName, strictTraceId, traceId);
  }

  @Override public Call<List<String>> getServiceNames() {
    if (!searchEnabled) return Call.emptyList();
    return SearchTableCall.keys(dynamoDB, searchTableName, SERVICE_SPAN_ENTITY_TYPE);
  }

  @Override public Call<List<String>> getSpanNames(String serviceName) {
    if (!searchEnabled) return Call.emptyList();
    serviceName = serviceName.toLowerCase(Locale.ROOT);
    return SearchTableCall.values(dynamoDB, searchTableName, SERVICE_SPAN_ENTITY_TYPE, serviceName);
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    if (endTs <= 0) throw new IllegalArgumentException("endTs <= 0");
    if (lookback <= 0) throw new IllegalArgumentException("lookback <= 0");
    return GetDependenciesCall.create(dynamoDB, endTs, lookback);
  }
}
