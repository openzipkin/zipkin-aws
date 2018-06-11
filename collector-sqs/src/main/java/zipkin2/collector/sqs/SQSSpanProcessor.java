/*
 * Copyright 2016-2018 The OpenZipkin Authors
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
package zipkin2.collector.sqs;

import com.amazonaws.AbortedException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.util.Base64;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import zipkin2.Callback;
import zipkin2.CheckResult;
import zipkin2.Component;
import zipkin2.collector.Collector;

final class SQSSpanProcessor extends Component implements Runnable {

  private static final Logger logger = Logger.getLogger(SQSSpanProcessor.class.getName());

  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final long DEFAULT_BACKOFF = 100;
  private static final long MAX_BACKOFF = 30000;

  private final AmazonSQS client;
  private final Collector collector;
  private final String queueUrl;
  private AtomicReference<CheckResult> status = new AtomicReference<>(CheckResult.OK);
  private final AtomicBoolean closed;
  private final ReceiveMessageRequest request;
  private long failureBackoff = DEFAULT_BACKOFF;

  SQSSpanProcessor(
      AmazonSQS client,
      Collector collector,
      String queueUrl,
      int waitTimeSeconds,
      int maxNumberOfMessages,
      AtomicBoolean closed) {
    this.client = client;
    this.collector = collector;
    this.queueUrl = queueUrl;
    this.closed = closed;

    request =
        new ReceiveMessageRequest(queueUrl)
            .withWaitTimeSeconds(waitTimeSeconds)
            .withMaxNumberOfMessages(maxNumberOfMessages);
  }

  @Override
  public CheckResult check() {
    return status.get();
  }

  @Override
  public void close() {
    // the collector owns closing of its resources so noop here
  }

  @Override
  public void run() {
    while (!closed.get()) {
      try {
        process(client.receiveMessage(request).getMessages());
        status.lazySet(CheckResult.OK);
        failureBackoff = DEFAULT_BACKOFF;
      } catch (AbortedException ae) {
        status.lazySet(CheckResult.failed(ae));
      } catch (Exception e) {
        logger.log(Level.WARNING, "sqs receive failed", e);
        status.lazySet(CheckResult.failed(e));

        // backoff on failures to avoid pinging SQS in a tight loop if there are failures.
        try {
          Thread.sleep(failureBackoff);
        } catch (InterruptedException ie) {
        } finally {
          failureBackoff = Math.max(failureBackoff * 2, MAX_BACKOFF);
        }
      }
    }
  }

  private void process(final List<Message> messages) {
    if (messages.size() == 0) return;

    final List<DeleteMessageBatchRequestEntry> toDelete = new ArrayList<>();
    int count = 0;
    for (Message message : messages) {
      final String deleteId = String.valueOf(count++);
      try {
        String stringBody = message.getBody();
        if (stringBody.isEmpty()) continue;
        // allow plain-text json, but permit base64 encoded thrift or json
        byte[] spans =
            stringBody.charAt(0) == '[' ? stringBody.getBytes(UTF_8) : Base64.decode(stringBody);
        collector.acceptSpans(
            spans,
            new Callback<Void>() {
              @Override
              public void onSuccess(Void value) {
                toDelete.add(
                    new DeleteMessageBatchRequestEntry(deleteId, message.getReceiptHandle()));
              }

              @Override
              public void onError(Throwable t) {
                logger.log(Level.WARNING, "collector accept failed", t);
                // for cases that are not recoverable just discard the message,
                // otherwise ignore so processing can be retried.
                if (t instanceof IllegalArgumentException) {
                  toDelete.add(
                      new DeleteMessageBatchRequestEntry(deleteId, message.getReceiptHandle()));
                }
              }
            });
      } catch (RuntimeException | Error e) {
        logger.log(Level.WARNING, "message decoding failed", e);
        toDelete.add(new DeleteMessageBatchRequestEntry(deleteId, message.getReceiptHandle()));
      }
    }

    delete(toDelete);
  }

  private DeleteMessageBatchResult delete(List<DeleteMessageBatchRequestEntry> entries) {
    return client.deleteMessageBatch(queueUrl, entries);
  }
}
