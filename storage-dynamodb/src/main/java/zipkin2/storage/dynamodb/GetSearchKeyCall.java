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
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.VALUE_INDEX;
import static zipkin2.storage.dynamodb.DynamoDBConstants.WILDCARD_FOR_INVERTED_INDEX_LOOKUP;

public class GetSearchKeyCall extends DynamoDBCall<List<String>> {
  private final Executor executor;
  private final AmazonDynamoDBAsync dynamoDB;
  private final String searchTableName;
  private final String type;
  private final List<String> excludedKeys;

  GetSearchKeyCall(Executor executor, AmazonDynamoDBAsync dynamoDB,
      String searchTableName, String type, List<String> excludedKeys) {
    super(executor);
    this.executor = executor;
    this.dynamoDB = dynamoDB;
    this.searchTableName = searchTableName;
    this.type = type;
    this.excludedKeys = excludedKeys;
  }

  @Override protected List<String> doExecute() {
    QueryResult result = dynamoDB.query(new QueryRequest(searchTableName)
        .withIndexName(VALUE_INDEX)
        .withSelect(Select.ALL_ATTRIBUTES)
        .withKeyConditionExpression(ENTITY_TYPE + " = :" + ENTITY_TYPE + " AND " + ENTITY_VALUE + " = :" + ENTITY_VALUE)
        .addExpressionAttributeValuesEntry(":" + ENTITY_TYPE, new AttributeValue().withS(type))
        .addExpressionAttributeValuesEntry(":" + ENTITY_VALUE, new AttributeValue().withS(WILDCARD_FOR_INVERTED_INDEX_LOOKUP))
    );
    return result.getItems().stream()
        .map(m -> m.get(ENTITY_KEY))
        .map(AttributeValue::getS)
        .filter(s -> !excludedKeys.contains(s))
        .collect(Collectors.toList());
  }

  @Override public Call<List<String>> clone() {
    return new GetSearchKeyCall(executor, dynamoDB, searchTableName, type, excludedKeys);
  }
}
