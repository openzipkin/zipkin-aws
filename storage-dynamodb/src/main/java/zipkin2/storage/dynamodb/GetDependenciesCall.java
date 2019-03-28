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
import com.amazonaws.services.dynamodbv2.model.Select;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.internal.AggregateCall;
import zipkin2.internal.DependencyLinker;

final class GetDependenciesCall extends DynamoDBCall.Query<List<DependencyLink>> {

  static Call<List<DependencyLink>> create(AmazonDynamoDBAsync dynamoDB, long endTs,
      long lookback) {
    LocalDateTime endOfDay = LocalDateTime.ofInstant(Instant.ofEpochMilli(endTs), ZoneId.of("UTC"))
        .withHour(23)
        .withMinute(59)
        .withSecond(59)
        .withNano(999999999);
    LocalDateTime start = LocalDateTime.ofInstant(Instant.ofEpochMilli(endTs).minusMillis(lookback),
        ZoneId.of("UTC"));

    QueryRequest request = new QueryRequest()
        .withTableName("zipkin-dependencies")
        .withSelect(Select.ALL_ATTRIBUTES)
        .withKeyConditionExpression("link_day = :link_day");

    List<GetDependenciesCall> calls = new ArrayList<>();
    while (endOfDay.isAfter(start) || endOfDay.isEqual(start)) {
      LocalDateTime endDayStart =
          LocalDateTime.of(endOfDay.getYear(), endOfDay.getMonth(), endOfDay.getDayOfMonth(), 0, 0,
              0, 0);
      request = request.clone().withExpressionAttributeValues(Collections.singletonMap(
          ":link_day",
          new AttributeValue().withS(
              String.valueOf(endDayStart.toInstant(ZoneOffset.UTC).toEpochMilli())
          )
      ));
      calls.add(new GetDependenciesCall(dynamoDB, request));
      endOfDay = endOfDay.minusDays(1);
    }

    if (calls.size() == 1) return calls.get(0);
    return new AggregateDependencyLinks(calls);
  }

  GetDependenciesCall(AmazonDynamoDBAsync dynamoDB, QueryRequest query) {
    super(dynamoDB, query);
  }

  @Override public Call<List<DependencyLink>> clone() {
    return new GetDependenciesCall(dynamoDB, request);
  }

  @Override public List<DependencyLink> map(List<Map<String, AttributeValue>> items) {
    List<DependencyLink> result = new ArrayList<>();
    for (Map<String, AttributeValue> map : items) {
      result.add(DependencyLink.newBuilder()
          .parent(map.get("parent").getS())
          .child(map.get("child").getS())
          .callCount(Long.valueOf(map.get("call_count").getN()))
          .errorCount(Long.valueOf(map.get("error_count").getN()))
          .build());
    }
    return result;
  }

  /** Since we cannot make a single call for all days of data, we are aggregating client-side */
  // TODO: cite more speicifically the technical limitation of the DynamoDB query api
  static final class AggregateDependencyLinks
      extends AggregateCall<List<DependencyLink>, List<DependencyLink>> {
    AggregateDependencyLinks(List<? extends Call<List<DependencyLink>>> calls) {
      super(calls);
    }

    @Override protected List<DependencyLink> newOutput() {
      return new ArrayList<>();
    }

    @Override protected void append(List<DependencyLink> input, List<DependencyLink> output) {
      output.addAll(input);
    }

    @Override protected List<DependencyLink> finish(List<DependencyLink> done) {
      return DependencyLinker.merge(done);
    }

    @Override protected boolean isEmpty(List<DependencyLink> output) {
      return output.isEmpty();
    }

    @Override public AggregateDependencyLinks clone() {
      return new AggregateDependencyLinks(cloneCalls());
    }
  }
}
