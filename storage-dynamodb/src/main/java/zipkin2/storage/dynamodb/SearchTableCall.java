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
import com.amazonaws.services.dynamodbv2.model.Select;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import zipkin2.Call;

import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.ENTITY_KEY;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.ENTITY_TYPE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.ENTITY_VALUE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.KEY_INDEX;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.UNKNOWN;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.VALUE_INDEX;
import static zipkin2.storage.dynamodb.DynamoDBConstants.WILDCARD_FOR_INVERTED_INDEX_LOOKUP;

final class SearchTableCall extends DynamoDBCall.Query<List<String>> {

  static Call<List<String>> keys(AmazonDynamoDBAsync dynamoDB, String searchTableName,
      String type) {
    return new SearchTableCall(dynamoDB,
        createQuery(searchTableName, VALUE_INDEX, type, ENTITY_VALUE,
            WILDCARD_FOR_INVERTED_INDEX_LOOKUP), true);
  }

  static Call<List<String>> values(AmazonDynamoDBAsync dynamoDB, String searchTableName,
      String type, String key) {
    return new SearchTableCall(dynamoDB,
        createQuery(searchTableName, KEY_INDEX, type, ENTITY_KEY, key), false);
  }

  static QueryRequest createQuery(String table, String index, String type, String column,
      String columnValue) {
    return new QueryRequest(table)
        .withIndexName(index)
        .withSelect(Select.ALL_ATTRIBUTES)
        .withKeyConditionExpression(
            ENTITY_TYPE + " = :" + ENTITY_TYPE + " AND " + column + " = :" + column)
        .addExpressionAttributeValuesEntry(":" + ENTITY_TYPE, new AttributeValue().withS(type))
        .addExpressionAttributeValuesEntry(":" + column, new AttributeValue().withS(columnValue));
  }

  final boolean returnKey;

  SearchTableCall(AmazonDynamoDBAsync dynamoDB, QueryRequest request, boolean returnKey) {
    super(dynamoDB, request);
    this.returnKey = returnKey;
  }

  @Override public Call<List<String>> clone() {
    return new SearchTableCall(dynamoDB, request, returnKey);
  }

  @Override public List<String> map(List<Map<String, AttributeValue>> items) {
    List<String> result = new ArrayList<>();
    for (Map<String, AttributeValue> map : items) {
      AttributeValue entityValue = map.get(returnKey ? ENTITY_KEY : ENTITY_VALUE);
      if (entityValue == null) continue;
      String s = entityValue.getS();
      if (WILDCARD_FOR_INVERTED_INDEX_LOOKUP.equals(s) || UNKNOWN.equals(s)) continue;
      result.add(s);
    }
    return result;
  }
}
