package zipkin2.storage.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class ZipkinDependenciesTable extends AbstractDynamoDBTable {
  ZipkinDependenciesTable(AmazonDynamoDB dynamoDB) {
    super(dynamoDB);
  }

  @Override protected CreateTableRequest createTable() {
    return new CreateTableRequest()
        .withTableName(tableName())
        .withAttributeDefinitions(
            new AttributeDefinition("link_day", ScalarAttributeType.S),
            new AttributeDefinition("parent_child", ScalarAttributeType.S)
        )
        .withKeySchema(
            new KeySchemaElement("link_day", KeyType.HASH),
            new KeySchemaElement("parent_child", KeyType.RANGE)
        )
        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
  }

  @Override protected String tableName() {
    return "zipkin-dependencies";
  }
}
