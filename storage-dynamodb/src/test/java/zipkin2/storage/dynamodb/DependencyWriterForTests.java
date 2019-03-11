package zipkin2.storage.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import java.util.List;
import java.util.stream.Collectors;
import zipkin2.DependencyLink;

final class DependencyWriterForTests {
  private final AmazonDynamoDBAsync dynamoDB;

  DependencyWriterForTests(AmazonDynamoDBAsync dynamoDB) {
    this.dynamoDB = dynamoDB;
  }

  void write(long midnight, List<DependencyLink> links) {
    List<WriteRequest> requests = convertToWriteRequests(midnight, links);

    while (!requests.isEmpty()) {
      int items = Math.min(25, requests.size());
      BatchWriteItemRequest request = new BatchWriteItemRequest()
          .addRequestItemsEntry("zipkin-dependencies", requests.subList(0, items));
      dynamoDB.batchWriteItem(request);
      requests.subList(0, items).clear();
    }
  }

  private List<WriteRequest> convertToWriteRequests(long midnight, List<DependencyLink> links) {
    return links.stream().map(link -> new WriteRequest().withPutRequest(
        new PutRequest()
            .addItemEntry("link_day", new AttributeValue().withS(String.valueOf(midnight)))
            .addItemEntry("parent_child", new AttributeValue().withS(link.parent() + "->" + link.child()))
            .addItemEntry("parent", new AttributeValue().withS(link.parent()))
            .addItemEntry("child", new AttributeValue().withS(link.child()))
            .addItemEntry("call_count", new AttributeValue().withN(String.valueOf(link.callCount())))
            .addItemEntry("error_count", new AttributeValue().withN(String.valueOf(link.errorCount())))
    )).collect(Collectors.toList());
  }
}
