package zipkin2.storage.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import java.util.List;
import java.util.concurrent.ExecutorService;
import zipkin2.Call;
import zipkin2.storage.AutocompleteTags;

import static zipkin2.storage.dynamodb.DynamoDBConstants.AUTOCOMPLETE_TAGS_TABLE_BASE_NAME;

final class DynamoDBAutocompleteTags implements AutocompleteTags {
  private final boolean searchEnabled;
  private final AmazonDynamoDBAsync dynamoDB;
  private final ExecutorService executorService;
  private final String autocompleteTagsTableName;

  DynamoDBAutocompleteTags(DynamoDBStorage.Builder builder) {
    this.searchEnabled = builder.searchEnabled;
    this.dynamoDB = builder.dynamoDB;
    this.executorService = builder.executorService;

    this.autocompleteTagsTableName = builder.tablePrefix + AUTOCOMPLETE_TAGS_TABLE_BASE_NAME;
  }

  @Override public Call<List<String>> getKeys() {
    if (!searchEnabled) {
      return Call.emptyList();
    }
    return new GetAutocompleteKeysCall(executorService, dynamoDB, autocompleteTagsTableName);
  }

  @Override public Call<List<String>> getValues(String key) {
    if (!searchEnabled) {
      return Call.emptyList();
    }
    return new GetAutocompleteValuesCall(executorService, dynamoDB, autocompleteTagsTableName, key);
  }
}
