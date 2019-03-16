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
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import static zipkin2.storage.dynamodb.DynamoDBConstants.SERVICE_SPAN_NAMES_TABLE_BASE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.ServiceSpanNames.INDEX_INVERTED;
import static zipkin2.storage.dynamodb.DynamoDBConstants.ServiceSpanNames.SERVICE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.ServiceSpanNames.SPAN;

public class ZipkinServiceSpanNamesTable extends AbstractDynamoDBTable {
  ZipkinServiceSpanNamesTable(AmazonDynamoDB dynamoDB) {
    super(dynamoDB);
  }

  @Override protected CreateTableRequest createTable() {
    return new CreateTableRequest()
        .withTableName(tableName())
        .withAttributeDefinitions(
            new AttributeDefinition(SERVICE, ScalarAttributeType.S),
            new AttributeDefinition(SPAN, ScalarAttributeType.S)
        )
        .withKeySchema(
            new KeySchemaElement(SERVICE, KeyType.HASH),
            new KeySchemaElement(SPAN, KeyType.RANGE)
        )
        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
        .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
            .withIndexName(INDEX_INVERTED)
            .withKeySchema(
                new KeySchemaElement(SPAN, KeyType.HASH),
                new KeySchemaElement(SERVICE, KeyType.RANGE)
            )
            .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
        );
  }

  @Override protected String tableName() {
    return "zipkin-" + SERVICE_SPAN_NAMES_TABLE_BASE_NAME;
  }

}
