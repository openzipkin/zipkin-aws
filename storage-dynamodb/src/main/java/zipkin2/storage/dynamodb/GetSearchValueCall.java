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
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.Select;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import zipkin2.Call;

import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.ENTITY_KEY;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.ENTITY_TYPE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.ENTITY_VALUE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.KEY_INDEX;
import static zipkin2.storage.dynamodb.DynamoDBConstants.WILDCARD_FOR_INVERTED_INDEX_LOOKUP;

public class GetSearchValueCall extends DynamoDBCall<List<String>> {
  private final Executor executor;
  private final AmazonDynamoDBAsync dynamoDB;
  private final String searchTableName;
  private final String type;
  private final String key;
  private final List<String> excludedValues;

  GetSearchValueCall(Executor executor, AmazonDynamoDBAsync dynamoDB,
      String searchTableName, String type, String key, List<String> excludedValues) {
    super(executor);
    this.executor = executor;
    this.dynamoDB = dynamoDB;
    this.searchTableName = searchTableName;
    this.type = type;
    this.key = key;
    this.excludedValues = excludedValues;
  }

  @Override protected List<String> doExecute() {
    QueryResult result = dynamoDB.query(new QueryRequest(searchTableName)
        .withIndexName(KEY_INDEX)
        .withSelect(Select.ALL_ATTRIBUTES)
        .withKeyConditionExpression(
            ENTITY_TYPE + " = :" + ENTITY_TYPE + " AND " + ENTITY_KEY + " = :" + ENTITY_KEY)
        .addExpressionAttributeValuesEntry(":" + ENTITY_TYPE, new AttributeValue().withS(type))
        .addExpressionAttributeValuesEntry(":" + ENTITY_KEY, new AttributeValue().withS(key))
    );
    return result.getItems().stream()
        .map(m -> m.get(ENTITY_VALUE))
        .map(AttributeValue::getS)
        .filter(s -> !s.equals(WILDCARD_FOR_INVERTED_INDEX_LOOKUP) && !excludedValues.contains(s))
        .collect(Collectors.toList());
  }

  @Override public Call<List<String>> clone() {
    return new GetSearchValueCall(executor, dynamoDB, searchTableName, type, key, excludedValues);
  }
}
