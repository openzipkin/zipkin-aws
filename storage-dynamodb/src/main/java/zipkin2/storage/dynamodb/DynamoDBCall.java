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

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import java.util.List;
import java.util.Map;
import zipkin2.Call;
import zipkin2.Callback;

/**
 * @param <I> Request type
 * @param <O> Result type
 * @param <V> Value type desired
 */
abstract class DynamoDBCall<I extends AmazonWebServiceRequest, O extends AmazonWebServiceResult<ResponseMetadata>, V>
    extends Call.Base<V> implements Call.Mapper<List<Map<String, AttributeValue>>, V> {

  static abstract class Query<V> extends DynamoDBCall<QueryRequest, QueryResult, V> {

    Query(AmazonDynamoDBAsync dynamoDB, QueryRequest query) {
      super(dynamoDB, query);
    }

    @Override List<Map<String, AttributeValue>> items(QueryResult result) {
      return result.getItems();
    }

    @Override QueryResult sync(QueryRequest request) {
      return dynamoDB.query(request);
    }

    @Override void async(QueryRequest request, AsyncHandler<QueryRequest, QueryResult> handler) {
      dynamoDB.queryAsync(request, handler);
    }
  }

  final AmazonDynamoDBAsync dynamoDB;
  final I request;

  abstract List<Map<String, AttributeValue>> items(O result);

  abstract O sync(I request);

  abstract void async(I request, AsyncHandler<I, O> handler);

  DynamoDBCall(AmazonDynamoDBAsync dynamoDB, I request) {
    this.dynamoDB = dynamoDB;
    this.request = request;
  }

  @Override protected final V doExecute() {
    return map(items(sync(request)));
  }

  @Override protected final void doEnqueue(Callback<V> callback) {
    try {
      async(request, new AsyncHandlerAdapter(callback));
    } catch (RuntimeException | Error e) {
      callback.onError(e);
      throw e;
    }
  }

  @Override public String toString() {
    return getClass().getSimpleName() + request;
  }

  final class AsyncHandlerAdapter implements AsyncHandler<I, O> {
    final Callback<V> delegate;

    AsyncHandlerAdapter(Callback<V> delegate) {
      this.delegate = delegate;
    }

    @Override public void onError(Exception e) {
      delegate.onError(e);
    }

    @Override public void onSuccess(I request, O result) {
      try {
        delegate.onSuccess(map(items(result)));
      } catch (Throwable e) {
        propagateIfFatal(e);
        delegate.onError(e);
      }
    }
  }
}
