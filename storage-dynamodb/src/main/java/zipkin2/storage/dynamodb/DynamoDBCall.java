package zipkin2.storage.dynamodb;

import java.util.concurrent.ExecutorService;
import zipkin2.Call;
import zipkin2.Callback;

abstract class DynamoDBCall<T> extends Call.Base<T> {
  private final ExecutorService executorService;

  DynamoDBCall(ExecutorService executorService) {
    this.executorService = executorService;
  }

  @Override final protected void doEnqueue(Callback<T> callback) {
    executorService.submit(() -> {
      try {
        callback.onSuccess(doExecute());
      } catch (Throwable t) {
        callback.onError(t);
      }
    });
  }
}
