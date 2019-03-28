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
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;

/**
 * We use the following tables:
 *
 * zipkin-spans zipkin-search zipkin-dependencies
 */
public final class DynamoDBStorage extends StorageComponent {
  public static Builder newBuilder(AmazonDynamoDBAsync client) {
    return new Builder(client);
  }

  public static final class Builder extends StorageComponent.Builder {
    final AmazonDynamoDBAsync client;
    boolean strictTraceId = true;
    boolean searchEnabled = true;
    List<String> autocompleteKeys = Collections.emptyList();
    String tablePrefix = "zipkin-";
    Executor executor = Executors.newCachedThreadPool();
    Duration dataTtl = Duration.ofDays(7);

    // TODO: we might consider changing this to build a client similar to how we do AWS senders
    Builder(AmazonDynamoDBAsync client) {
      if (client == null) throw new NullPointerException("client == null");
      this.client = client;
    }

    @Override public Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    @Override public Builder searchEnabled(boolean searchEnabled) {
      this.searchEnabled = searchEnabled;
      return this;
    }

    @Override public Builder autocompleteKeys(List<String> autocompleteKeys) {
      if (autocompleteKeys == null) throw new NullPointerException("autocompleteKeys == null");
      this.autocompleteKeys = autocompleteKeys;
      return this;
    }

    public Builder tablePrefix(String tablePrefix) {
      this.tablePrefix = tablePrefix;
      return this;
    }

    public Builder executor(Executor executor) {
      this.executor = executor;
      return this;
    }

    public Builder dataTtl(Duration dataTtl) {
      this.dataTtl = dataTtl;
      return this;
    }

    @Override public DynamoDBStorage build() {
      return new DynamoDBStorage(this);
    }
  }

  final AmazonDynamoDBAsync client;
  final Executor executor;
  final DynamoDBSpanStore dynamoDBSpanStore;
  final DynamoDBSpanConsumer dynamoDBSpanConsumer;
  final DynamoDBAutocompleteTags dynamoDBAutocompleteTags;

  DynamoDBStorage(Builder builder) {
    client = builder.client;
    executor = builder.executor;
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

  @Override public void close() {
    // TODO: we probably want to manage the dynamodb client so we can close it properly
  }
}
