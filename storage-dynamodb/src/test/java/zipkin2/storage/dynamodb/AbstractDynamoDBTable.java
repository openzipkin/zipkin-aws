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
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractDynamoDBTable {

  private final AmazonDynamoDB dynamoDB;

  AbstractDynamoDBTable(AmazonDynamoDB dynamoDB) {
    this.dynamoDB = dynamoDB;
  }

  void create() {
    if (!tableExists()) {
      dynamoDB.createTable(createTable());
    }
  }

  void drop() {
    if (tableExists()) {
      dynamoDB.deleteTable(tableName());
    }
  }

  private boolean tableExists() {
    return dynamoDB.listTables().getTableNames().contains(tableName());
  }

  protected abstract CreateTableRequest createTable();
  protected abstract String tableName();
}
