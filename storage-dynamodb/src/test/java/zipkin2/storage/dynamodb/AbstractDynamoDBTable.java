package zipkin2.storage.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractDynamoDBTable {

  private final AmazonDynamoDB dynamoDB;

  AbstractDynamoDBTable(AmazonDynamoDB dynamoDB) {
    this.dynamoDB = dynamoDB;
  }

  void create() {
    if (!tableExists()) {
      dynamoDB.createTable(createTable());
    }
  }

  void drop() {
    if (tableExists()) {
      dynamoDB.deleteTable(tableName());
    }
  }

  private boolean tableExists() {
    return dynamoDB.listTables().getTableNames().contains(tableName());
  }

  protected abstract CreateTableRequest createTable();
  protected abstract String tableName();
}
