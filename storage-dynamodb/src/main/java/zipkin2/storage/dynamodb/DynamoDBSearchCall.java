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
