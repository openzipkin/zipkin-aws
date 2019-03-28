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
package zipkin2.storage.dynamodb.internal;

import java.util.ArrayList;
import java.util.List;
import zipkin2.Call;
import zipkin2.internal.AggregateCall;

public final class CollectIntoList<T> extends AggregateCall<List<T>, List<List<T>>> {
  public CollectIntoList(List<? extends Call<List<T>>> calls) {
    super(calls);
  }

  @Override protected List<List<T>> newOutput() {
    return new ArrayList<>();
  }

  @Override protected void append(List<T> input, List<List<T>> output) {
    output.add(input);
  }

  @Override protected boolean isEmpty(List<List<T>> output) {
    return output.isEmpty();
  }

  @Override public CollectIntoList<T> clone() {
    return new CollectIntoList<>(cloneCalls());
  }
}
