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

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import zipkin2.Call;
import zipkin2.storage.dynamodb.internal.AggregateIntoSet;

import static java.util.Arrays.asList;
import static zipkin2.storage.dynamodb.DynamoDBConstants.FIELD_DELIMITER;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.ANNOTATIONS;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.LOCAL_SERVICE_SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.REMOTE_SERVICE_SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_DURATION;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_TIMESTAMP;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_TIMESTAMP_ID;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TAG_PREFIX;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TRACE_ID;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TRACE_ID_64;

abstract class GetTraceIdsCall<I extends AmazonWebServiceRequest, O extends AmazonWebServiceResult<ResponseMetadata>>
    extends DynamoDBCall<I, O, Set<String>> {
  static final BigInteger MAX_TIMESTAMP_ID = new BigInteger("ffffffffffffffff", 16);

  static final String TIMESTAMP_ID_UPPER_BOUND = ":timestamp_id_upper_bound";
  static final String TIMESTAMP_ID_LOWER_BOUND = ":timestamp_id_lower_bound";
  static final String SPAN_TIMESTAMP_ID_FILTER = SPAN_TIMESTAMP_ID
      + " BETWEEN "
      + TIMESTAMP_ID_LOWER_BOUND
      + " AND "
      + TIMESTAMP_ID_UPPER_BOUND;
  static final String QUERY_PROJECTION_EXPRESSION =
      String.join(", ", TRACE_ID, TRACE_ID_64, SPAN_TIMESTAMP);

  static Call<Set<String>> create(boolean strictTraceId,
      AmazonDynamoDBAsync dynamoDB, String spansTableName,
      zipkin2.storage.QueryRequest queryRequest) {
    Map<String, AttributeValue> expressionAttributeValues = new LinkedHashMap<>();
    Map<String, String> expressionAttributeNames = new LinkedHashMap<>();
    List<String> filters = new ArrayList<>();

    // We store a timestamp UUID-like value, where the upper 64 bits are the timestamp and lower 64
    // are random, so we can get a range of "timestamp_id" by using endTs and lookback, the
    // highest is calculated as: (endTs << 64 + 0xffffffffffffffff), and the lowest is calculated
    // as: ((endTs - lookback) << 64)
    BigInteger timestampIdUpperBound =
        BigInteger.valueOf(queryRequest.endTs()).shiftLeft(64).add(MAX_TIMESTAMP_ID);
    BigInteger timestampIdLowerBound =
        BigInteger.valueOf(queryRequest.endTs() - queryRequest.lookback()).shiftLeft(64);

    expressionAttributeValues.put(TIMESTAMP_ID_UPPER_BOUND,
        new AttributeValue().withN(timestampIdUpperBound.toString()));
    expressionAttributeValues.put(TIMESTAMP_ID_LOWER_BOUND,
        new AttributeValue().withN(timestampIdLowerBound.toString()));

    // Sets the duration filters for the query
    // if min is not null and max is not null then span_duration is between them
    //    if they are equal, we filter to span_duration == min
    // if min is not null and max is then span_duration is >= min
    if (queryRequest.minDuration() != null) {
      expressionAttributeValues.put(":min_duration",
          new AttributeValue().withN(queryRequest.minDuration().toString()));
      if (queryRequest.maxDuration() != null) {
        if (queryRequest.maxDuration().equals(queryRequest.minDuration())) {
          filters.add(SPAN_DURATION + " = :min_duration");
        } else {
          expressionAttributeValues.put(":max_duration",
              new AttributeValue().withN(queryRequest.maxDuration().toString()));
          filters.add(SPAN_DURATION + " BETWEEN :min_duration AND :max_duration");
        }
      } else {
        filters.add(SPAN_DURATION + " >= :min_duration");
      }
    }

    // Sets tag and annotation filters
    // For each item in the query set up an equality check, if it does not have a value we check for
    // tag exists OR IN annotations, otherwise we check tag=value
    int i = 0;
    for (Map.Entry<String, String> tag : queryRequest.annotationQuery().entrySet()) {
      expressionAttributeNames.put("#tag_key" + i, TAG_PREFIX + tag.getKey());
      if (tag.getValue().isEmpty()) {
        expressionAttributeValues.put(":annotation" + i,
            new AttributeValue().withS(tag.getKey()));
        filters.add(
            String.format("(attribute_exists(#tag_key%d) OR contains(%s, :annotation%d))", i,
                ANNOTATIONS, i));
      } else {
        filters.add(String.format("#tag_key%d = :tag_value%d", i, i));
        expressionAttributeValues.put(":tag_value" + i,
            new AttributeValue().withS(tag.getValue()));
      }
      i++;
    }

    // This signals that we can use our secondary indexes on service_names and/or span_names
    if (queryRequest.serviceName() != null || queryRequest.spanName() != null) {
      if (queryRequest.serviceName() != null) {
        String key = queryRequest.spanName() != null
            ? queryRequest.serviceName() + FIELD_DELIMITER + queryRequest.spanName()
            : queryRequest.serviceName();
        QueryRequest localRequest = createQuery(spansTableName, LOCAL_SERVICE_SPAN_NAME, key,
            expressionAttributeValues, expressionAttributeNames, filters);
        QueryRequest remoteRequest = createQuery(spansTableName, REMOTE_SERVICE_SPAN_NAME, key,
            expressionAttributeValues, expressionAttributeNames, filters);

        return new AggregateIntoSet<>(asList(
            new QueryIndexForTraceIds(dynamoDB, localRequest, strictTraceId, queryRequest.limit()),
            new QueryIndexForTraceIds(dynamoDB, remoteRequest, strictTraceId, queryRequest.limit())
        ));
      } else {
        QueryRequest request = createQuery(spansTableName, SPAN_NAME, queryRequest.spanName(),
            expressionAttributeValues, expressionAttributeNames, filters);
        return new QueryIndexForTraceIds(dynamoDB, request, strictTraceId, queryRequest.limit());
      }
    } else {
      // We have to scan now because we don't have an index to use based on only endTs & lookback
      filters.add(SPAN_TIMESTAMP_ID_FILTER);

      ScanRequest request = new ScanRequest(spansTableName)
          .withProjectionExpression(QUERY_PROJECTION_EXPRESSION)
          .withExpressionAttributeValues(expressionAttributeValues)
          .withFilterExpression(String.join(" AND ", filters));
      if (expressionAttributeNames.size() > 0) {
        request.withExpressionAttributeNames(expressionAttributeNames);
      }

      return new ScanSpansForTraceIds(dynamoDB, request, strictTraceId, queryRequest.limit());
    }
  }

  static QueryRequest createQuery(String spansTableName, String index, String key,
      Map<String, AttributeValue> expressionAttributeValues,
      Map<String, String> expressionAttributeNames, List<String> filters) {
    QueryRequest queryRequest = new QueryRequest(spansTableName)
        .withIndexName(index)
        .withProjectionExpression(QUERY_PROJECTION_EXPRESSION)
        .withKeyConditionExpression(
            String.format("%s = :%s AND %s", index, index, SPAN_TIMESTAMP_ID_FILTER))
        .withScanIndexForward(false);

    queryRequest.addExpressionAttributeValuesEntry(":" + index, new AttributeValue().withS(key));
    for (Map.Entry<String, AttributeValue> attributeValue : expressionAttributeValues.entrySet()) {
      queryRequest.addExpressionAttributeValuesEntry(attributeValue.getKey(),
          attributeValue.getValue());
    }

    for (Map.Entry<String, String> attributeName : expressionAttributeNames.entrySet()) {
      queryRequest.addExpressionAttributeNamesEntry(attributeName.getKey(),
          attributeName.getValue());
    }

    if (!filters.isEmpty()) {
      queryRequest.withFilterExpression(String.join(" AND ", filters));
    }

    return queryRequest;
  }

  static final class QueryIndexForTraceIds extends GetTraceIdsCall<QueryRequest, QueryResult> {

    QueryIndexForTraceIds(AmazonDynamoDBAsync dynamoDB, QueryRequest request, boolean strictTraceId,
        int limit) {
      super(dynamoDB, request, strictTraceId, limit);
    }

    @Override List<Map<String, AttributeValue>> items(QueryResult result) {
      return result.getItems();
    }

    @Override QueryResult sync(QueryRequest request) {
      return dynamoDB.query(request);
    }

    @Override void async(QueryRequest request, AsyncHandler<QueryRequest, QueryResult> handler) {
      dynamoDB.queryAsync(request, handler);
    }

    @Override public Call<Set<String>> clone() {
      return new QueryIndexForTraceIds(dynamoDB, request, strictTraceId, limit);
    }
  }

  static final class ScanSpansForTraceIds extends GetTraceIdsCall<ScanRequest, ScanResult> {

    ScanSpansForTraceIds(AmazonDynamoDBAsync dynamoDB, ScanRequest request, boolean strictTraceId,
        int limit) {
      super(dynamoDB, request, strictTraceId, limit);
    }

    @Override List<Map<String, AttributeValue>> items(ScanResult result) {
      return result.getItems();
    }

    @Override ScanResult sync(ScanRequest request) {
      return dynamoDB.scan(request);
    }

    @Override void async(ScanRequest request, AsyncHandler<ScanRequest, ScanResult> handler) {
      dynamoDB.scanAsync(request, handler);
    }

    @Override public Call<Set<String>> clone() {
      return new ScanSpansForTraceIds(dynamoDB, request, strictTraceId, limit);
    }
  }

  final boolean strictTraceId;
  final int limit;

  GetTraceIdsCall(AmazonDynamoDBAsync dynamoDB, I request, boolean strictTraceId, int limit) {
    super(dynamoDB, request);
    this.strictTraceId = strictTraceId;
    this.limit = limit;
  }

  @Override public Set<String> map(List<Map<String, AttributeValue>> items) {
    String traceIdField = strictTraceId ? TRACE_ID : TRACE_ID_64;
    Map<String, Map<String, AttributeValue>> keep = new LinkedHashMap<>();

    for (Map<String, AttributeValue> row : items) {
      String traceId = row.get(traceIdField).getS();
      if (keep.containsKey(traceId)) {
        if (Long.valueOf(row.get(SPAN_TIMESTAMP).getN()) > Long.valueOf(
            keep.get(traceId).get(SPAN_TIMESTAMP).getN())) {
          keep.put(traceId, row);
        }
      } else {
        keep.put(traceId, row);
      }
    }

    List<Map<String, AttributeValue>> result = new ArrayList<>(keep.values());
    result.sort((m1, m2) -> Long.valueOf(m2.get(SPAN_TIMESTAMP).getN())
        .compareTo(Long.valueOf(m1.get(SPAN_TIMESTAMP).getN())));
    return result.subList(0, Math.min(result.size(), limit)).stream()
        .map(m -> strictTraceId ? m.get(TRACE_ID) : m.get(TRACE_ID_64))
        .map(AttributeValue::getS)
        .collect(Collectors.toSet());
  }
}
