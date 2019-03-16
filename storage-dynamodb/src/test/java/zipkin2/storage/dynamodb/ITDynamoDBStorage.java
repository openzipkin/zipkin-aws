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

import java.util.List;
import java.util.concurrent.Executors;
import org.junit.ClassRule;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import zipkin2.Span;
import zipkin2.storage.StorageComponent;

@RunWith(Enclosed.class)
public class ITDynamoDBStorage {

  @ClassRule public static DynamoDBRule dynamoDBRule = new DynamoDBRule();

  public static class ITDependencies extends zipkin2.storage.ITDependencies {
    DynamoDBStorage storage;
    DependencyWriterForTests dependencyWriter;

    public ITDependencies() {
      storage = new DynamoDBStorage(new DynamoDBStorage.Builder()
          .dynamoDB(dynamoDBRule.dynamoDB())
          .executorService(Executors.newSingleThreadExecutor()));
      dependencyWriter = new DependencyWriterForTests(dynamoDBRule.dynamoDB());
    }

    protected DynamoDBStorage storage() {
      return this.storage;
    }

    public void clear() {
      dynamoDBRule.cleanUp();
    }

    @Override protected void processDependencies(List<Span> spans) throws Exception {
      aggregateLinks(spans).forEach(dependencyWriter::write);
    }
  }

  public static class ITAutocompleteTags extends zipkin2.storage.ITAutocompleteTags {
    protected StorageComponent.Builder storageBuilder() {
      return new DynamoDBStorage.Builder()
          .dynamoDB(dynamoDBRule.dynamoDB())
          .executorService(Executors.newSingleThreadExecutor());
    }

    public void clear() {
      dynamoDBRule.cleanUp();
    }
  }

  public static class ITStrictTraceIdFalse extends zipkin2.storage.ITStrictTraceIdFalse {
    DynamoDBStorage storage;

    public ITStrictTraceIdFalse() {
      storage = new DynamoDBStorage(new DynamoDBStorage.Builder()
          .dynamoDB(dynamoDBRule.dynamoDB())
          .strictTraceId(false)
          .executorService(Executors.newSingleThreadExecutor()));
    }

    protected DynamoDBStorage storage() {
      return this.storage;
    }

    public void clear() {
      dynamoDBRule.cleanUp();
    }
  }

  public static class ITSearchEnabledFalse extends zipkin2.storage.ITSearchEnabledFalse {
    DynamoDBStorage storage;

    public ITSearchEnabledFalse() {
      storage = new DynamoDBStorage(new DynamoDBStorage.Builder()
          .dynamoDB(dynamoDBRule.dynamoDB())
          .searchEnabled(false)
          .executorService(Executors.newSingleThreadExecutor()));
    }

    protected DynamoDBStorage storage() {
      return this.storage;
    }

    public void clear() {
      dynamoDBRule.cleanUp();
    }
  }

  public static class ITSpanStore extends zipkin2.storage.ITSpanStore {
    DynamoDBStorage storage;

    public ITSpanStore() {
      storage = new DynamoDBStorage(new DynamoDBStorage.Builder()
          .dynamoDB(dynamoDBRule.dynamoDB())
          .executorService(Executors.newSingleThreadExecutor()));
    }

    protected DynamoDBStorage storage() {
      return this.storage;
    }

    public void clear() {
      dynamoDBRule.cleanUp();
    }
  }
}
