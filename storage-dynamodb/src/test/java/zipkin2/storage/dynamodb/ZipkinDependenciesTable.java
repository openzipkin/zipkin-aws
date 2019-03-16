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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class ZipkinDependenciesTable extends AbstractDynamoDBTable {
  ZipkinDependenciesTable(AmazonDynamoDB dynamoDB) {
    super(dynamoDB);
  }

  @Override protected CreateTableRequest createTable() {
    return new CreateTableRequest()
        .withTableName(tableName())
        .withAttributeDefinitions(
            new AttributeDefinition("link_day", ScalarAttributeType.S),
            new AttributeDefinition("parent_child", ScalarAttributeType.S)
        )
        .withKeySchema(
            new KeySchemaElement("link_day", KeyType.HASH),
            new KeySchemaElement("parent_child", KeyType.RANGE)
        )
        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
  }

  @Override protected String tableName() {
    return "zipkin-dependencies";
  }
}
