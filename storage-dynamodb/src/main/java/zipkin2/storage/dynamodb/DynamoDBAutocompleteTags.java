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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import zipkin2.Call;
import zipkin2.storage.AutocompleteTags;

import static zipkin2.storage.dynamodb.DynamoDBConstants.SEARCH_TABLE_BASE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.AUTOCOMPLETE_TAG_ENTITY_TYPE;

final class DynamoDBAutocompleteTags implements AutocompleteTags {
  private final boolean searchEnabled;
  private final AmazonDynamoDBAsync dynamoDB;
  private final Executor executor;
  private final String searchTableName;

  DynamoDBAutocompleteTags(DynamoDBStorage.Builder builder) {
    this.searchEnabled = builder.searchEnabled;
    this.dynamoDB = builder.client;
    this.executor = builder.executor;

    this.searchTableName = builder.tablePrefix + SEARCH_TABLE_BASE_NAME;
  }

  @Override public Call<List<String>> getKeys() {
    if (!searchEnabled) {
      return Call.emptyList();
    }
    return new GetSearchKeyCall(executor, dynamoDB, searchTableName,
        AUTOCOMPLETE_TAG_ENTITY_TYPE,
        Collections.emptyList());
  }

  @Override public Call<List<String>> getValues(String key) {
    if (!searchEnabled) {
      return Call.emptyList();
    }
    return new GetSearchValueCall(executor, dynamoDB, searchTableName,
        AUTOCOMPLETE_TAG_ENTITY_TYPE,
        key, Collections.emptyList());
  }
}
