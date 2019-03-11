package zipkin2.storage.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.Select;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import zipkin2.Call;

import static zipkin2.storage.dynamodb.DynamoDBConstants.AutocompleteTags.INDEX_INVERTED;
import static zipkin2.storage.dynamodb.DynamoDBConstants.AutocompleteTags.TAG;
import static zipkin2.storage.dynamodb.DynamoDBConstants.AutocompleteTags.VALUE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.WILDCARD_FOR_INVERTED_INDEX_LOOKUP;

final class GetAutocompleteKeysCall extends DynamoDBCall<List<String>> {
  private final ExecutorService executorService;
  private final AmazonDynamoDBAsync dynamoDB;
  private final String autocompleteTagsTableName;

  GetAutocompleteKeysCall(ExecutorService executorService, AmazonDynamoDBAsync dynamoDB,
      String autocompleteTagsTableName) {
    super(executorService);
    this.executorService = executorService;
    this.dynamoDB = dynamoDB;
    this.autocompleteTagsTableName = autocompleteTagsTableName;
  }

  @Override protected List<String> doExecute() {
    QueryResult result = dynamoDB.query(
        new com.amazonaws.services.dynamodbv2.model.QueryRequest(autocompleteTagsTableName)
            .withIndexName(INDEX_INVERTED)
            .withSelect(Select.ALL_ATTRIBUTES)
            .withKeyConditionExpression(VALUE + " = :" + VALUE)
            .withExpressionAttributeValues(
                Collections.singletonMap(":" + VALUE, new AttributeValue().withS(
                    WILDCARD_FOR_INVERTED_INDEX_LOOKUP)))
    );
    return result.getItems().stream()
        .map(m -> m.get(TAG))
        .map(AttributeValue::getS)
        .collect(Collectors.toList());
  }

  @Override public Call<List<String>> clone() {
    return new GetAutocompleteKeysCall(executorService, dynamoDB, autocompleteTagsTableName);
  }
}
