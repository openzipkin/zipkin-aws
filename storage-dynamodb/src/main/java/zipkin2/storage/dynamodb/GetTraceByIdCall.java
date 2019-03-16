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
import com.amazonaws.services.dynamodbv2.model.Select;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;

import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_BLOB;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TRACE_ID;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TRACE_ID_64;

final class GetTraceByIdCall extends DynamoDBCall<List<Span>> {
  private final ExecutorService executorService;
  private final AmazonDynamoDBAsync dynamoDB;
  private final String spansTableName;
  private final boolean strictTraceId;
  private final String traceId;

  GetTraceByIdCall(ExecutorService executorService, AmazonDynamoDBAsync dynamoDB,
      String spansTableName, boolean strictTraceId,
      String traceId) {
    super(executorService);
    this.executorService = executorService;
    this.dynamoDB = dynamoDB;
    this.spansTableName = spansTableName;
    this.strictTraceId = strictTraceId;
    this.traceId = traceId;
  }

  @Override protected List<Span> doExecute() {
    QueryRequest dynamoRequest = strictTraceId ? strictTraceIdQuery() : lenientTraceIdQuery();
    QueryResult result = dynamoDB.query(dynamoRequest);

    List<Span> spans = new ArrayList<>(result.getCount());
    for (Map<String, AttributeValue> row : result.getItems()) {
      spans.add(SpanBytesDecoder.PROTO3.decodeOne(row.get(SPAN_BLOB).getB().array()));
    }
    return spans;
  }

  private QueryRequest strictTraceIdQuery() {
    return new QueryRequest(spansTableName)
        .withSelect(Select.ALL_ATTRIBUTES)
        .withKeyConditionExpression(TRACE_ID + " = :" + TRACE_ID)
        .withExpressionAttributeValues(
            Collections.singletonMap(":" + TRACE_ID, new AttributeValue().withS(traceId)));
  }

  private QueryRequest lenientTraceIdQuery() {
    return new QueryRequest(spansTableName)
        .withIndexName(TRACE_ID_64)
        .withSelect(Select.ALL_ATTRIBUTES)
        .withKeyConditionExpression(TRACE_ID_64 + " = :" + TRACE_ID_64)
        .withExpressionAttributeValues(
            Collections.singletonMap(":" + TRACE_ID_64,
                new AttributeValue().withS(traceId.substring(Math.max(0, traceId.length() - 16)))));
  }

  @Override public Call<List<Span>> clone() {
    return new GetTraceByIdCall(executorService, dynamoDB, spansTableName, strictTraceId, traceId);
  }
}
