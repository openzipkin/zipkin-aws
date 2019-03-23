package zipkin2.storage.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.Select;
import java.util.List;
import java.util.concurrent.Executor;

import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.ENTITY_TYPE;

abstract class DynamoDBSearchCall extends DynamoDBCall<List<String>> {
  DynamoDBSearchCall(Executor executor) {
    super(executor);
  }

  QueryRequest createQuery(String table, String index, String type, String column, String columnValue) {
    return new QueryRequest(table)
        .withIndexName(index)
        .withSelect(Select.ALL_ATTRIBUTES)
        .withKeyConditionExpression(
            ENTITY_TYPE + " = :" + ENTITY_TYPE + " AND " + column + " = :" + column)
        .addExpressionAttributeValuesEntry(":" + ENTITY_TYPE, new AttributeValue().withS(type))
        .addExpressionAttributeValuesEntry(":" + column, new AttributeValue().withS(columnValue));
  }
}
