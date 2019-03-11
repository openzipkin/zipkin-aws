package zipkin2.storage.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.Select;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import zipkin2.Call;

import static zipkin2.storage.dynamodb.DynamoDBConstants.AutocompleteTags.TAG;
import static zipkin2.storage.dynamodb.DynamoDBConstants.AutocompleteTags.VALUE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.WILDCARD_FOR_INVERTED_INDEX_LOOKUP;

final class GetAutocompleteValuesCall extends DynamoDBCall<List<String>> {
  private final ExecutorService executorService;
  private final AmazonDynamoDBAsync dynamoDB;
  private final String autocompleteTagsTableName;
  private final String key;

  GetAutocompleteValuesCall(ExecutorService executorService, AmazonDynamoDBAsync dynamoDB,
      String autocompleteTagsTableName, String key) {
    super(executorService);
    this.executorService = executorService;
    this.dynamoDB = dynamoDB;
    this.autocompleteTagsTableName = autocompleteTagsTableName;
    this.key = key;
  }

  @Override protected List<String> doExecute() throws IOException {
    QueryResult result = dynamoDB.query(
        new com.amazonaws.services.dynamodbv2.model.QueryRequest(autocompleteTagsTableName)
            .withSelect(Select.ALL_ATTRIBUTES)
            .withKeyConditionExpression(TAG + " = :" + TAG)
            .withExpressionAttributeValues(
                Collections.singletonMap(":" + TAG, new AttributeValue().withS(key)))
    );
    return result.getItems().stream()
        .map(m -> m.get(VALUE))
        .map(AttributeValue::getS)
        .filter(s -> !s.equals(WILDCARD_FOR_INVERTED_INDEX_LOOKUP))
        .collect(Collectors.toList());
  }

  @Override public Call<List<String>> clone() {
    return new GetAutocompleteValuesCall(executorService, dynamoDB, autocompleteTagsTableName, key);
  }
}
