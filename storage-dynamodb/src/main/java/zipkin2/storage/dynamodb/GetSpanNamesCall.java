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
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.Select;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import zipkin2.Call;

import static zipkin2.storage.dynamodb.DynamoDBConstants.ServiceSpanNames.SERVICE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.ServiceSpanNames.SPAN;
import static zipkin2.storage.dynamodb.DynamoDBConstants.ServiceSpanNames.UNKNOWN;
import static zipkin2.storage.dynamodb.DynamoDBConstants.WILDCARD_FOR_INVERTED_INDEX_LOOKUP;

final class GetSpanNamesCall extends DynamoDBCall<List<String>> {
  private final Executor executor;
  private final AmazonDynamoDBAsync dynamoDB;
  private final String serviceSpanNamesTableName;
  private final String serviceName;

  GetSpanNamesCall(Executor executor, AmazonDynamoDBAsync dynamoDB,
      String serviceSpanNamesTableName, String serviceName) {
    super(executor);
    this.executor = executor;
    this.dynamoDB = dynamoDB;
    this.serviceSpanNamesTableName = serviceSpanNamesTableName;
    this.serviceName = serviceName;
  }

  @Override protected List<String> doExecute() {
    QueryResult result = dynamoDB.query(
        new com.amazonaws.services.dynamodbv2.model.QueryRequest(serviceSpanNamesTableName)
            .withSelect(Select.ALL_ATTRIBUTES)
            .withKeyConditionExpression(SERVICE + " = :" + SERVICE)
            .withExpressionAttributeValues(
                Collections.singletonMap(":" + SERVICE, new AttributeValue().withS(serviceName)))
    );
    return result.getItems().stream()
        .map(m -> m.get(SPAN))
        .map(AttributeValue::getS)
        .filter(s -> !s.equals(WILDCARD_FOR_INVERTED_INDEX_LOOKUP) && !s.equals(UNKNOWN))
        .collect(Collectors.toList());
  }

  @Override public Call<List<String>> clone() {
    return new GetSpanNamesCall(executor, dynamoDB, serviceSpanNamesTableName, serviceName);
  }
}
