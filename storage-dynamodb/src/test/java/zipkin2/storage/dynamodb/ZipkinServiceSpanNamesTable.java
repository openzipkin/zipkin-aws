package zipkin2.storage.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import static zipkin2.storage.dynamodb.DynamoDBConstants.SERVICE_SPAN_NAMES_TABLE_BASE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.ServiceSpanNames.INDEX_INVERTED;
import static zipkin2.storage.dynamodb.DynamoDBConstants.ServiceSpanNames.SERVICE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.ServiceSpanNames.SPAN;

public class ZipkinServiceSpanNamesTable extends AbstractDynamoDBTable {
  ZipkinServiceSpanNamesTable(AmazonDynamoDB dynamoDB) {
    super(dynamoDB);
  }

  @Override protected CreateTableRequest createTable() {
    return new CreateTableRequest()
        .withTableName(tableName())
        .withAttributeDefinitions(
            new AttributeDefinition(SERVICE, ScalarAttributeType.S),
            new AttributeDefinition(SPAN, ScalarAttributeType.S)
        )
        .withKeySchema(
            new KeySchemaElement(SERVICE, KeyType.HASH),
            new KeySchemaElement(SPAN, KeyType.RANGE)
        )
        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
        .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
            .withIndexName(INDEX_INVERTED)
            .withKeySchema(
                new KeySchemaElement(SPAN, KeyType.HASH),
                new KeySchemaElement(SERVICE, KeyType.RANGE)
            )
            .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
        );
  }

  @Override protected String tableName() {
    return "zipkin-" + SERVICE_SPAN_NAMES_TABLE_BASE_NAME;
  }

}
