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

import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.LOCAL_SERVICE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.LOCAL_SERVICE_NAME_SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.REMOTE_SERVICE_NAME_SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.REMOTE_SERVICE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TIMESTAMP_SPAN_ID;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TRACE_ID;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TRACE_ID_64;

public class ZipkinSpansTable extends AbstractDynamoDBTable {

  ZipkinSpansTable(AmazonDynamoDB dynamoDB) {
    super(dynamoDB);
  }

  @Override protected CreateTableRequest createTable() {
    return new CreateTableRequest()
        .withTableName(tableName())
        .withAttributeDefinitions(
            new AttributeDefinition(TRACE_ID, ScalarAttributeType.S),
            new AttributeDefinition(TRACE_ID_64, ScalarAttributeType.S),
            new AttributeDefinition(TIMESTAMP_SPAN_ID, ScalarAttributeType.N),
            new AttributeDefinition(SPAN_NAME, ScalarAttributeType.S),
            new AttributeDefinition(LOCAL_SERVICE_NAME, ScalarAttributeType.S),
            new AttributeDefinition(LOCAL_SERVICE_NAME_SPAN_NAME, ScalarAttributeType.S),
            new AttributeDefinition(REMOTE_SERVICE_NAME, ScalarAttributeType.S),
            new AttributeDefinition(REMOTE_SERVICE_NAME_SPAN_NAME, ScalarAttributeType.S)
        )
        .withKeySchema(
            new KeySchemaElement(TRACE_ID, KeyType.HASH),
            new KeySchemaElement(TIMESTAMP_SPAN_ID, KeyType.RANGE)
        )
        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
        .withGlobalSecondaryIndexes(
            new GlobalSecondaryIndex()
                .withIndexName(TRACE_ID_64)
                .withKeySchema(
                    new KeySchemaElement(TRACE_ID_64, KeyType.HASH),
                    new KeySchemaElement(TIMESTAMP_SPAN_ID, KeyType.RANGE)
                )
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)),
            new GlobalSecondaryIndex()
                .withIndexName(SPAN_NAME)
                .withKeySchema(
                    new KeySchemaElement(SPAN_NAME, KeyType.HASH),
                    new KeySchemaElement(TIMESTAMP_SPAN_ID, KeyType.RANGE)
                )
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)),
            new GlobalSecondaryIndex()
                .withIndexName(LOCAL_SERVICE_NAME)
                .withKeySchema(
                    new KeySchemaElement(LOCAL_SERVICE_NAME, KeyType.HASH),
                    new KeySchemaElement(TIMESTAMP_SPAN_ID, KeyType.RANGE)
                )
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)),
            new GlobalSecondaryIndex()
                .withIndexName(REMOTE_SERVICE_NAME)
                .withKeySchema(
                    new KeySchemaElement(REMOTE_SERVICE_NAME, KeyType.HASH),
                    new KeySchemaElement(TIMESTAMP_SPAN_ID, KeyType.RANGE)
                )
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)),
            new GlobalSecondaryIndex()
                .withIndexName(LOCAL_SERVICE_NAME_SPAN_NAME)
                .withKeySchema(
                    new KeySchemaElement(LOCAL_SERVICE_NAME_SPAN_NAME, KeyType.HASH),
                    new KeySchemaElement(TIMESTAMP_SPAN_ID, KeyType.RANGE)
                )
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)),
            new GlobalSecondaryIndex()
                .withIndexName(REMOTE_SERVICE_NAME_SPAN_NAME)
                .withKeySchema(
                    new KeySchemaElement(REMOTE_SERVICE_NAME_SPAN_NAME, KeyType.HASH),
                    new KeySchemaElement(TIMESTAMP_SPAN_ID, KeyType.RANGE)
                )
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
        );
  }

  @Override protected String tableName() {
    return "zipkin-" + DynamoDBConstants.SPANS_TABLE_BASE_NAME;
  }
}
