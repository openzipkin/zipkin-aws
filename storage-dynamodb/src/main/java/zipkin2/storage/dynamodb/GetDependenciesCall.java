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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import zipkin2.Call;
import zipkin2.DependencyLink;

final class GetDependenciesCall extends DynamoDBCall<List<DependencyLink>> {
  private final Executor executor;
  private final AmazonDynamoDBAsync dynamoDB;
  private final long endTs;
  private final long lookback;

  GetDependenciesCall(Executor executor, AmazonDynamoDBAsync dynamoDB, long endTs,
      long lookback) {
    super(executor);
    this.executor = executor;
    this.dynamoDB = dynamoDB;
    this.endTs = endTs;
    this.lookback = lookback;
  }

  @Override protected List<DependencyLink> doExecute() {
    LocalDateTime endOfDay = LocalDateTime.ofInstant(Instant.ofEpochMilli(endTs), ZoneId.of("UTC"))
        .withHour(23)
        .withMinute(59)
        .withSecond(59)
        .withNano(999999999);
    LocalDateTime start = LocalDateTime.ofInstant(Instant.ofEpochMilli(endTs).minusMillis(lookback),
        ZoneId.of("UTC"));

    List<DependencyLink> links = new ArrayList<>();
    QueryRequest request = new QueryRequest()
        .withTableName("zipkin-dependencies")
        .withSelect(Select.ALL_ATTRIBUTES)
        .withKeyConditionExpression("link_day = :link_day");

    while (endOfDay.isAfter(start) || endOfDay.isEqual(start)) {
      LocalDateTime endDayStart =
          LocalDateTime.of(endOfDay.getYear(), endOfDay.getMonth(), endOfDay.getDayOfMonth(), 0, 0,
              0, 0);
      request = request.withExpressionAttributeValues(Collections.singletonMap(
          ":link_day",
          new AttributeValue().withS(
              String.valueOf(endDayStart.toInstant(ZoneOffset.UTC).toEpochMilli())
          )
      ));
      QueryResult result = dynamoDB.query(request);
      links.addAll(result.getItems().stream().map(this::fromMap).collect(Collectors.toList()));
      endOfDay = endOfDay.minusDays(1);
    }

    return merge(links);
  }

  @Override public Call<List<DependencyLink>> clone() {
    return new GetDependenciesCall(executor, dynamoDB, endTs, lookback);
  }

  private DependencyLink fromMap(Map<String, AttributeValue> map) {
    return DependencyLink.newBuilder()
        .parent(map.get("parent").getS())
        .child(map.get("child").getS())
        .callCount(Long.valueOf(map.get("call_count").getN()))
        .errorCount(Long.valueOf(map.get("error_count").getN()))
        .build();
  }

  private List<DependencyLink> merge(List<DependencyLink> links) {
    Map<String, DependencyLink> linkMap = new HashMap<>();
    links.forEach(l -> {
      String key = l.parent() + "-" + l.child();
      if (linkMap.containsKey(key)) {
        DependencyLink existing = linkMap.get(key);
        linkMap.put(key, existing.toBuilder()
            .callCount(existing.callCount() + l.callCount())
            .errorCount(existing.errorCount() + l.errorCount())
            .build());
      } else {
        linkMap.put(key, l);
      }
    });
    return new ArrayList<>(linkMap.values());
  }
}
