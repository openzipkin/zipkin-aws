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
import java.math.BigInteger;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import zipkin2.internal.HexCodec;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;

/**
 * We use the following tables:
 *
 * zipkin-spans zipkin-service-span-names zipkin-dependencies zipkin-autocomplete-tags
 */
public final class DynamoDBStorage extends StorageComponent {
  private DynamoDBSpanStore dynamoDBSpanStore;
  private DynamoDBSpanConsumer dynamoDBSpanConsumer;
  private DynamoDBAutocompleteTags dynamoDBAutocompleteTags;

  DynamoDBStorage(DynamoDBStorage.Builder builder) {
    dynamoDBSpanStore = new DynamoDBSpanStore(builder);
    dynamoDBSpanConsumer = new DynamoDBSpanConsumer(builder);
    dynamoDBAutocompleteTags = new DynamoDBAutocompleteTags(builder);
  }

  @Override public SpanStore spanStore() {
    return dynamoDBSpanStore;
  }

  @Override public SpanConsumer spanConsumer() {
    return dynamoDBSpanConsumer;
  }

  @Override public AutocompleteTags autocompleteTags() {
    return dynamoDBAutocompleteTags;
  }

  public static final class Builder extends StorageComponent.Builder {
    boolean strictTraceId = true;
    boolean searchEnabled = true;
    List<String> autocompleteKeys = Collections.emptyList();
    AmazonDynamoDBAsync dynamoDB;
    String tablePrefix = "zipkin-";
    Executor executor = Executors.newCachedThreadPool();
    Duration dataTtl = Duration.ofDays(7);

    /** {@inheritDoc} */
    @Override
    public DynamoDBStorage.Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    @Override
    public DynamoDBStorage.Builder searchEnabled(boolean searchEnabled) {
      this.searchEnabled = searchEnabled;
      return this;
    }

    @Override
    public DynamoDBStorage.Builder autocompleteKeys(List<String> autocompleteKeys) {
      if (autocompleteKeys == null) throw new NullPointerException("autocompleteKeys == null");
      this.autocompleteKeys = autocompleteKeys;
      return this;
    }

    public DynamoDBStorage.Builder dynamoDB(AmazonDynamoDBAsync dynamoDB) {
      this.dynamoDB = dynamoDB;
      return this;
    }

    public DynamoDBStorage.Builder tablePrefix(String tablePrefix) {
      this.tablePrefix = tablePrefix;
      return this;
    }

    public DynamoDBStorage.Builder executorService(Executor executor) {
      this.executor = executor;
      return this;
    }

    public DynamoDBStorage.Builder dataTtl(Duration dataTtl) {
      this.dataTtl = dataTtl;
      return this;
    }

    @Override public StorageComponent build() {
      if (dynamoDB == null) {
        throw new IllegalStateException("A AmazonDynamoDBAsync client is required");
      }
      return new DynamoDBStorage(this);
    }
  }
}
