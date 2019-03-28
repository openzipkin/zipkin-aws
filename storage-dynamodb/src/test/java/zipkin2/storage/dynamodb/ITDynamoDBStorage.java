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
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import zipkin2.Span;

@RunWith(Enclosed.class)
public class ITDynamoDBStorage {

  /** Written intentionally to allow you to run a single nested method via the CLI. See README */
  static DynamoDBRule classRule() {
    return new DynamoDBRule("amazon/dynamodb-local");
  }

  public static class ITSpanStore extends zipkin2.storage.ITSpanStore {
    @ClassRule public static DynamoDBRule dynamodb = classRule();

    @Override public DynamoDBStorage storage() {
      return dynamodb.storage;
    }

    @Test @Ignore @Override public void deduplicates() {
      // currently we don't deduplicate writes of the same doc.
      // Not sure DynamoDB can (via checksum etc).
    }

    @Before public void clear() {
      dynamodb.clear();
    }
  }

  public static class ITSearchEnabledFalse extends zipkin2.storage.ITSearchEnabledFalse {
    @ClassRule public static DynamoDBRule dynamodb = classRule();

    @Override public DynamoDBStorage storage() {
      return dynamodb.computeStorageBuilder().searchEnabled(false).build();
    }

    @Before public void clear() throws Exception {
      dynamodb.clear();
    }
  }

  public static class ITStrictTraceIdFalse extends zipkin2.storage.ITStrictTraceIdFalse {
    @ClassRule public static DynamoDBRule dynamodb = classRule();

    @Override public DynamoDBStorage storage() {
      return dynamodb.computeStorageBuilder().strictTraceId(false).build();
    }

    @Before public void clear() throws Exception {
      dynamodb.clear();
    }
  }

  public static class ITDependencies extends zipkin2.storage.ITDependencies {
    @ClassRule public static DynamoDBRule dynamodb = classRule();

    @Override protected DynamoDBStorage storage() {
      return dynamodb.storage;
    }

    @Override protected void processDependencies(List<Span> spans) {
      DependencyWriterForTests dependencyWriter = new DependencyWriterForTests(storage().client);
      aggregateLinks(spans).forEach(dependencyWriter::write);
    }

    @Override public void clear() throws Exception {
      dynamodb.clear();
    }
  }
}
