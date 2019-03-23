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

import static zipkin2.storage.dynamodb.DynamoDBConstants.FIELD_DELIMITER;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.ANNOTATIONS;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_DURATION;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.LOCAL_SERVICE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.LOCAL_SERVICE_SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.REMOTE_SERVICE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.REMOTE_SERVICE_SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_BLOB;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TAG_PREFIX;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_TIMESTAMP;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_TIMESTAMP_ID;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TRACE_ID;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TRACE_ID_64;

final class GetTracesForQueryCall extends DynamoDBCall<List<List<Span>>> {
  private static final BigInteger MAX_TIMESTAMP_ID = new BigInteger("ffffffffffffffff", 16);

  private static final String TIMESTAMP_ID_UPPER_BOUND = ":timestamp_id_upper_bound";
  private static final String TIMESTAMP_ID_LOWER_BOUND = ":timestamp_id_lower_bound";
  private static final String SPAN_TIMESTAMP_ID_FILTER = SPAN_TIMESTAMP_ID + " BETWEEN " + TIMESTAMP_ID_LOWER_BOUND + " AND " + TIMESTAMP_ID_UPPER_BOUND;
  private static final String QUERY_PROJECTION_EXPRESSION = String.join(", ", TRACE_ID, TRACE_ID_64, SPAN_TIMESTAMP);

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
    List<Map<String, AttributeValue>> rows = new ArrayList<>();
    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    Map<String, String> expressionAttributeNames = new HashMap<>();
    List<String> filters = new ArrayList<>();

    // We store a timestamp UUID-like value, where the upper 64 bits are the timestamp and lower 64
    // are random, so we can get a range of "timestamp_id" by using endTs and lookback, the
    // highest is calculated as: (endTs << 64 + 0xffffffffffffffff), and the lowest is calculated
    // as: ((endTs - lookback) << 64)
    BigInteger timestampIdUpperBound = BigInteger.valueOf(queryRequest.endTs()).shiftLeft(64).add(MAX_TIMESTAMP_ID);
    BigInteger timestampIdLowerBound = BigInteger.valueOf(queryRequest.endTs() - queryRequest.lookback()).shiftLeft(64);

    expressionAttributeValues.put(TIMESTAMP_ID_UPPER_BOUND, new AttributeValue().withN(timestampIdUpperBound.toString()));
    expressionAttributeValues.put(TIMESTAMP_ID_LOWER_BOUND, new AttributeValue().withN(timestampIdLowerBound.toString()));

    // Sets the duration filters for the query
    // if min is not null and max is not null then span_duration is between them
    //    if they are equal, we filter to span_duration == min
    // if min is not null and max is then span_duration is >= min
    if (queryRequest.minDuration() != null) {
      expressionAttributeValues.put(":min_duration", new AttributeValue().withN(queryRequest.minDuration().toString()));
      if (queryRequest.maxDuration() != null) {
        if (queryRequest.maxDuration().equals(queryRequest.minDuration())) {
          filters.add(SPAN_DURATION + " = :min_duration");
        } else {
          expressionAttributeValues.put(":max_duration", new AttributeValue().withN(queryRequest.maxDuration().toString()));
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
        expressionAttributeValues.put(":annotation" + i, new AttributeValue().withS(tag.getKey()));
        filters.add(String.format("(attribute_exists(#tag_key%d) OR contains(%s, :annotation%d))", i, ANNOTATIONS, i));
      } else {
        filters.add(String.format("#tag_key%d = :tag_value%d", i, i));
        expressionAttributeValues.put(":tag_value" + i, new AttributeValue().withS(tag.getValue()));
      }
      i++;
    }

    // This signals that we can use our secondary indexes on service_names and/or span_names
    if (queryRequest.serviceName() != null || queryRequest.spanName() != null) {
      if (queryRequest.serviceName() != null) {
        if (queryRequest.spanName() != null) {
          rows.addAll(
              getAllLocalAndRemoteRows(
                  LOCAL_SERVICE_SPAN_NAME,
                  REMOTE_SERVICE_SPAN_NAME,
                  queryRequest.serviceName() + FIELD_DELIMITER + queryRequest.spanName(),
                  expressionAttributeValues,
                  expressionAttributeNames,
                  filters
              ));
        } else {
          rows.addAll(
              getAllLocalAndRemoteRows(
                  LOCAL_SERVICE_NAME,
                  REMOTE_SERVICE_NAME,
                  queryRequest.serviceName(),
                  expressionAttributeValues,
                  expressionAttributeNames,
                  filters
              ));
        }
      } else {
        boolean hasMore = true;
        Map<String, AttributeValue> lastKey = Collections.emptyMap();
        while (hasMore) {
          lastKey = queryRows(rows, SPAN_NAME, queryRequest.spanName(), lastKey, expressionAttributeValues, expressionAttributeNames, filters);
          hasMore = lastKey != null && !lastKey.isEmpty();
        }
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
    return new GetTracesForQueryCall(executor, strictTraceId, dynamoDB, spansTableName, queryRequest);
  }

  private List<Map<String, AttributeValue>> getAllLocalAndRemoteRows(String localIndex, String remoteIndex, String key, Map<String, AttributeValue> expressionAttributeValues, Map<String, String> expressionAttributeNames, List<String> filters) {
    List<Map<String, AttributeValue>> rows = new ArrayList<>();
    boolean moreLocal = true;
    Map<String, AttributeValue> localLastKey = Collections.emptyMap();
    boolean moreRemote = true;
    Map<String, AttributeValue> remoteLastKey = Collections.emptyMap();
    while (moreLocal || moreRemote) {
      if (moreLocal) {
        localLastKey = queryRows(rows, localIndex, key, localLastKey, expressionAttributeValues, expressionAttributeNames, filters);
        moreLocal = localLastKey != null && !localLastKey.isEmpty();
      }

      if (moreRemote) {
        remoteLastKey = queryRows(rows, remoteIndex, key, remoteLastKey, expressionAttributeValues, expressionAttributeNames, filters);
        moreRemote = remoteLastKey != null && !remoteLastKey.isEmpty();
      }
    }
    return rows;
  }

  private Map<String, AttributeValue> queryRows(List<Map<String, AttributeValue>> rowCollection, String index, String value, Map<String, AttributeValue> lastEvaluatedKey, Map<String, AttributeValue> expressionAttributeValues, Map<String, String> expressionAttributeNames, List<String> filters) {
    QueryRequest request = createQuery(index, value, expressionAttributeValues, expressionAttributeNames, filters);
    if (!lastEvaluatedKey.isEmpty()) {
      request.withExclusiveStartKey(lastEvaluatedKey);
    }
    QueryResult result = dynamoDB.query(request);
    rowCollection.addAll(result.getItems());
    // undo previous changes so future queries don't break
    return result.getLastEvaluatedKey();
  }

  private QueryRequest createQuery(String index, String key, Map<String, AttributeValue> expressionAttributeValues, Map<String, String> expressionAttributeNames, List<String> filters) {
    QueryRequest queryRequest = new QueryRequest(spansTableName)
        .withIndexName(index)
        .withProjectionExpression(QUERY_PROJECTION_EXPRESSION)
        .withKeyConditionExpression(String.format("%s = :%s AND %s", index, index, SPAN_TIMESTAMP_ID_FILTER))
        .withScanIndexForward(false);

    queryRequest.addExpressionAttributeValuesEntry(":" + index, new AttributeValue().withS(key));
    for (Map.Entry<String, AttributeValue> attributeValue : expressionAttributeValues.entrySet()) {
      queryRequest.addExpressionAttributeValuesEntry(attributeValue.getKey(), attributeValue.getValue());
    }

    for (Map.Entry<String, String> attributeName : expressionAttributeNames.entrySet()) {
      queryRequest.addExpressionAttributeNamesEntry(attributeName.getKey(), attributeName.getValue());
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

    return result;
  }

  private List<Span> getSpansForTraceId(String traceId) {
    QueryRequest request = new QueryRequest(spansTableName)
        .withProjectionExpression(TRACE_ID + ", " + TRACE_ID_64 + ", " + SPAN_BLOB)
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
