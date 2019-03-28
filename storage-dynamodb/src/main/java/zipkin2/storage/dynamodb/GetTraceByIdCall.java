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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;

import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_BLOB;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TRACE_ID;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TRACE_ID_64;

final class GetTraceByIdCall extends DynamoDBCall.Query<List<Span>> {
  static Call<List<Span>> create(AmazonDynamoDBAsync dynamoDB, String spansTableName,
      boolean strictTraceId, String traceId) {
    if (strictTraceId) {
      return new GetTraceByIdCall(dynamoDB, strictTraceIdQuery(spansTableName, traceId));
    }
    return new GetTraceByIdCall(dynamoDB, lenientTraceIdQuery(spansTableName, traceId));
  }

  static QueryRequest strictTraceIdQuery(String spansTableName, String traceId) {
    return new QueryRequest(spansTableName)
        .withProjectionExpression(TRACE_ID + ", " + TRACE_ID_64 + ", " + SPAN_BLOB)
        .withKeyConditionExpression(TRACE_ID + " = :" + TRACE_ID)
        .withExpressionAttributeValues(
            Collections.singletonMap(":" + TRACE_ID, new AttributeValue().withS(traceId)));
  }

  static QueryRequest lenientTraceIdQuery(String spansTableName, String traceId) {
    return new QueryRequest(spansTableName)
        .withIndexName(TRACE_ID_64)
        .withProjectionExpression(TRACE_ID + ", " + TRACE_ID_64 + ", " + SPAN_BLOB)
        .withKeyConditionExpression(TRACE_ID_64 + " = :" + TRACE_ID_64)
        .withExpressionAttributeValues(
            Collections.singletonMap(":" + TRACE_ID_64,
                new AttributeValue().withS(traceId.substring(Math.max(0, traceId.length() - 16)))));
  }

  GetTraceByIdCall(AmazonDynamoDBAsync dynamoDB, QueryRequest request) {
    super(dynamoDB, request);
  }

  @Override public Call<List<Span>> clone() {
    return new GetTraceByIdCall(dynamoDB, request);
  }

  @Override public List<Span> map(List<Map<String, AttributeValue>> items) {
    List<Span> result = new ArrayList<>();
    for (Map<String, AttributeValue> row : items) {
      result.add(SpanBytesDecoder.PROTO3.decodeOne(row.get(SPAN_BLOB).getB().array()));
    }
    return result;
  }
}
