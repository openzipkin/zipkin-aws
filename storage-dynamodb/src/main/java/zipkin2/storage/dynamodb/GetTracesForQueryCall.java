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
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;

import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.ANNOTATIONS;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.DURATION;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.LOCAL_SERVICE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.LOCAL_SERVICE_NAME_SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.REMOTE_SERVICE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.REMOTE_SERVICE_NAME_SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_BLOB;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TAG_PREFIX;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TIMESTAMP;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TIMESTAMP_SPAN_ID;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TRACE_ID;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TRACE_ID_64;

final class GetTracesForQueryCall extends DynamoDBCall<List<List<Span>>> {
  private static final BigInteger LAST_SPAN_ID = new BigInteger("ffffffffffffffff", 16);

  private final Executor executor;
  private final boolean strictTraceId;
  private final AmazonDynamoDBAsync dynamoDB;
  private final String spansTableName;
  private final zipkin2.storage.QueryRequest queryRequest;

  GetTracesForQueryCall(Executor executor, boolean strictTraceId,
      AmazonDynamoDBAsync dynamoDB, String spansTableName,
      zipkin2.storage.QueryRequest queryRequest) {
    super(executor);
    this.executor = executor;
    this.strictTraceId = strictTraceId;
    this.dynamoDB = dynamoDB;
    this.spansTableName = spansTableName;
    this.queryRequest = queryRequest;
  }

  @Override protected List<List<Span>> doExecute() {
    BigInteger timestampSpanIdUpperBound =
        BigInteger.valueOf(queryRequest.endTs()).shiftLeft(64).add(LAST_SPAN_ID);
    BigInteger timestampSpanIdLowerBound =
        BigInteger.valueOf(queryRequest.endTs() - queryRequest.lookback()).shiftLeft(64);

    List<Map<String, AttributeValue>> rows = new ArrayList<>();
    Map<String, AttributeValue> filterExpressionAttributes = new HashMap<>();
    Map<String, String> expressionAttributeNames = new HashMap<>();
    List<String> filters = new ArrayList<>();

    expressionAttributeNames.put("#timestamp", TIMESTAMP);
    filterExpressionAttributes.put(":timestamp_upper",
        new AttributeValue().withN(timestampSpanIdUpperBound.toString()));
    filterExpressionAttributes.put(":timestamp_lower",
        new AttributeValue().withN(timestampSpanIdLowerBound.toString()));

    if (queryRequest.minDuration() != null) {
      expressionAttributeNames.put("#dur", DURATION);
      filterExpressionAttributes.put(":min_duration",
          new AttributeValue().withN(queryRequest.minDuration().toString()));
      if (queryRequest.maxDuration() != null) {
        if (queryRequest.maxDuration().equals(queryRequest.minDuration())) {
          filters.add("#dur = :min_duration");
        } else {
          filterExpressionAttributes.put(":max_duration",
              new AttributeValue().withN(queryRequest.maxDuration().toString()));
          filters.add("#dur BETWEEN :min_duration AND :max_duration");
        }
      } else {
        filters.add("#dur >= :min_duration");
      }
    }

    int i = 0;
    for (Map.Entry<String, String> tag : queryRequest.annotationQuery().entrySet()) {
      expressionAttributeNames.put("#tag_key" + i, TAG_PREFIX + tag.getKey());
      if (tag.getValue().isEmpty()) {
        filterExpressionAttributes.put(":annotation" + i, new AttributeValue().withS(tag.getKey()));
        filters.add(
            String.format("(attribute_exists(#tag_key%d) OR contains(%s, :annotation%d))", i,
                ANNOTATIONS, i));
      } else {
        filters.add(String.format("#tag_key%d = :tag_value%d", i, i));
        filterExpressionAttributes.put(":tag_value" + i,
            new AttributeValue().withS(tag.getValue()));
      }
      i++;
    }

    if (queryRequest.serviceName() != null || queryRequest.spanName() != null) {
      // USE dynamodb query
      if (queryRequest.serviceName() != null) {
        if (queryRequest.spanName() != null) {
          QueryRequest query = getQueryForGlobalSecondaryIndex(LOCAL_SERVICE_NAME_SPAN_NAME,
              queryRequest.serviceName() + "###" + queryRequest.spanName(),
              expressionAttributeNames, filterExpressionAttributes, filters);
          QueryResult result = dynamoDB.query(query);
          rows.addAll(result.getItems());
          filterExpressionAttributes.remove(":" + LOCAL_SERVICE_NAME_SPAN_NAME);

          query = getQueryForGlobalSecondaryIndex(REMOTE_SERVICE_NAME_SPAN_NAME,
              queryRequest.serviceName() + "###" + queryRequest.spanName(),
              expressionAttributeNames, filterExpressionAttributes, filters);
          result = dynamoDB.query(query);
          rows.addAll(result.getItems());
        } else {
          // only service names
          QueryRequest query = getQueryForGlobalSecondaryIndex(LOCAL_SERVICE_NAME,
              queryRequest.serviceName(), expressionAttributeNames, filterExpressionAttributes,
              filters);
          QueryResult result = dynamoDB.query(query);
          rows.addAll(result.getItems());
          filterExpressionAttributes.remove(":" + LOCAL_SERVICE_NAME);

          query = getQueryForGlobalSecondaryIndex(REMOTE_SERVICE_NAME, queryRequest.serviceName(),
              expressionAttributeNames,
              filterExpressionAttributes, filters);
          result = dynamoDB.query(query);
          rows.addAll(result.getItems());
        }
      } else {
        QueryRequest query = getQueryForGlobalSecondaryIndex(SPAN_NAME, queryRequest.spanName(),
            expressionAttributeNames,
            filterExpressionAttributes, filters);
        QueryResult result = dynamoDB.query(query);
        rows.addAll(result.getItems());
      }
    } else {
      filters.add(TIMESTAMP_SPAN_ID + " BETWEEN :timestamp_lower AND :timestamp_upper");

      ScanRequest request = new ScanRequest(spansTableName)
          .withProjectionExpression(TRACE_ID + ", " + TRACE_ID_64 + ", #timestamp")
          .withExpressionAttributeValues(filterExpressionAttributes)
          .withFilterExpression(String.join(" AND ", filters));
      if (expressionAttributeNames.size() > 0) {
        request.withExpressionAttributeNames(expressionAttributeNames);
      }

      ScanResult result = dynamoDB.scan(request);

      rows.addAll(result.getItems());
    }

    List<Map<String, AttributeValue>> aggregate = aggregateRows(rows);
    if (aggregate.isEmpty()) {
      return Collections.emptyList();
    } else {
      Set<String> traceIds =
          aggregate.subList(0, Math.min(aggregate.size(), queryRequest.limit())).stream()
              .map(m -> strictTraceId ? m.get(TRACE_ID) : m.get(TRACE_ID_64))
              .map(AttributeValue::getS)
              .collect(Collectors.toSet());
      return traceIds.stream().map(this::getSpansForTraceId).collect(Collectors.toList());
    }
  }

  @Override public Call<List<List<Span>>> clone() {
    return new GetTracesForQueryCall(executor, strictTraceId, dynamoDB, spansTableName,
        queryRequest);
  }

  private QueryRequest getQueryForGlobalSecondaryIndex(String indexName,
      String key, Map<String, String> expressionAttributeNames,
      Map<String, AttributeValue> filterExpressionAttributes,
      List<String> filters) {

    filterExpressionAttributes.put(":" + indexName, new AttributeValue().withS(key));

    QueryRequest queryRequest = new QueryRequest(spansTableName)
        .withProjectionExpression(TRACE_ID + ", " + TRACE_ID_64 + ", #timestamp")
        .withScanIndexForward(false)
        .withIndexName(indexName)
        .withKeyConditionExpression(indexName
            + " = :"
            + indexName
            + " AND "
            + TIMESTAMP_SPAN_ID
            + " BETWEEN :timestamp_lower AND :timestamp_upper")
        .withExpressionAttributeValues(filterExpressionAttributes);
    if (expressionAttributeNames.size() > 0) {
      queryRequest.withExpressionAttributeNames(expressionAttributeNames);
    }
    if (!filters.isEmpty()) {
      queryRequest.withFilterExpression(String.join(" AND ", filters));
    }
    return queryRequest;
  }

  private List<Map<String, AttributeValue>> aggregateRows(List<Map<String, AttributeValue>> input) {
    String traceIdField = strictTraceId ? TRACE_ID : TRACE_ID_64;
    Map<String, Map<String, AttributeValue>> keep = new HashMap<>();

    for (Map<String, AttributeValue> row : input) {
      String traceId = row.get(traceIdField).getS();
      if (keep.containsKey(traceId)) {
        if (Long.valueOf(row.get(TIMESTAMP).getN()) > Long.valueOf(
            keep.get(traceId).get(TIMESTAMP).getN())) {
          keep.put(traceId, row);
        }
      } else {
        keep.put(traceId, row);
      }
    }

    List<Map<String, AttributeValue>> result = new ArrayList<>(keep.values());
    result.sort((m1, m2) -> Long.valueOf(m2.get(TIMESTAMP).getN())
        .compareTo(Long.valueOf(m1.get(TIMESTAMP).getN())));

    return result;
  }

  private List<Span> getSpansForTraceId(String traceId) {
    QueryRequest request = new QueryRequest(spansTableName)
        .withAttributesToGet(TRACE_ID, TRACE_ID_64, SPAN_BLOB)
        .withExpressionAttributeValues(
            Collections.singletonMap(":" + TRACE_ID, new AttributeValue().withS(traceId)));

    if (strictTraceId) {
      request.withKeyConditionExpression(TRACE_ID + " = :" + TRACE_ID);
    } else {
      request.withIndexName(TRACE_ID_64)
          .withKeyConditionExpression(TRACE_ID_64 + " = :" + TRACE_ID);
    }

    QueryResult result = dynamoDB.query(request);
    return result.getItems().stream()
        .map(m -> m.get(SPAN_BLOB).getB().array())
        .map(SpanBytesDecoder.PROTO3::decodeOne)
        .collect(Collectors.toList());
  }
}
