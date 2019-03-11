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
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import zipkin2.Call;
import zipkin2.DependencyLink;

final class GetDependenciesCall extends DynamoDBCall<List<DependencyLink>> {
  private final ExecutorService executorService;
  private final AmazonDynamoDBAsync dynamoDB;
  private final long endTs;
  private final long lookback;

  GetDependenciesCall(ExecutorService executorService, AmazonDynamoDBAsync dynamoDB, long endTs,
      long lookback) {
    super(executorService);
    this.executorService = executorService;
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
    return new GetDependenciesCall(executorService, dynamoDB, endTs, lookback);
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
