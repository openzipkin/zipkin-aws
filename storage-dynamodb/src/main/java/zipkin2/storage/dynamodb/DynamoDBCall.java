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

import java.util.concurrent.Executor;
import zipkin2.Call;
import zipkin2.Callback;

abstract class DynamoDBCall<T> extends Call.Base<T> {
  private final Executor executor;

  DynamoDBCall(Executor executor) {
    this.executor = executor;
  }

  @Override final protected void doEnqueue(Callback<T> callback) {
    executor.execute(() -> {
      try {
        callback.onSuccess(doExecute());
      } catch (Throwable t) {
        callback.onError(t);
      }
    });
  }
}
