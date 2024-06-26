/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.collector.sqs;

import com.amazonaws.AbortedException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.util.Base64;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
import zipkin2.collector.CollectorMetrics;

final class SQSSpanProcessor extends Component implements Runnable {

  private static final Logger logger = Logger.getLogger(SQSSpanProcessor.class.getName());

  private static final Charset UTF_8 = StandardCharsets.UTF_8;
  private static final long DEFAULT_BACKOFF = 100;
  private static final long MAX_BACKOFF = 30000;

  final AmazonSQS client;
  final Collector collector;
  final CollectorMetrics metrics;
  final String queueUrl;
  final AtomicReference<CheckResult> status = new AtomicReference<>(CheckResult.OK);
  final AtomicBoolean closed;
  final ReceiveMessageRequest request;
  long failureBackoff = DEFAULT_BACKOFF;

  SQSSpanProcessor(SQSCollector sqsCollector) {
    client = sqsCollector.client();
    collector = sqsCollector.collector;
    metrics = sqsCollector.metrics;
    queueUrl = sqsCollector.queueUrl;
    closed = sqsCollector.closed;
    request = new ReceiveMessageRequest(queueUrl)
        .withWaitTimeSeconds(sqsCollector.waitTimeSeconds)
        .withMaxNumberOfMessages(sqsCollector.maxNumberOfMessages);
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
    if (messages.isEmpty()) return;

    final List<DeleteMessageBatchRequestEntry> toDelete = new ArrayList<>();
    int count = 0;
    for (Message message : messages) {
      final String deleteId = String.valueOf(count++);
      try {
        String stringBody = message.getBody();
        if (stringBody.isEmpty() || stringBody.equals("[]")) continue;
        // allow plain-text json, but permit base64 encoded thrift or json
        byte[] serialized =
            stringBody.charAt(0) == '[' ? stringBody.getBytes(UTF_8) : Base64.decode(stringBody);
        metrics.incrementMessages();
        metrics.incrementBytes(serialized.length);
        collector.acceptSpans(serialized, new Callback<>() {
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

    if (!toDelete.isEmpty()) {
      delete(toDelete);
    }
  }

  private void delete(List<DeleteMessageBatchRequestEntry> entries) {
    client.deleteMessageBatch(queueUrl, entries);
  }

  @Override public String toString() {
    return "SQSSpanProcessor{queueUrl=" + queueUrl + "}";
  }
}
