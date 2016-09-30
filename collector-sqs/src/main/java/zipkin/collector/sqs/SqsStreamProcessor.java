package zipkin.collector.sqs;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import zipkin.Codec;
import zipkin.collector.Collector;
import zipkin.collector.CollectorMetrics;
import zipkin.storage.Callback;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by ddcbdevins on 9/29/16.
 */
public class SqsStreamProcessor implements Runnable, Closeable {

    final AmazonSQSClient client;
    final String sqsQueueUrl;
    final int waitTimeSeconds;
    final Collector collector;
    final CollectorMetrics metrics;

    private final AtomicBoolean run = new AtomicBoolean(true);

    SqsStreamProcessor(AmazonSQSClient client, String sqsQueueUrl, int waitTimeSeconds, Collector collector, CollectorMetrics metrics) {
        this.client = client;
        this.sqsQueueUrl = sqsQueueUrl;
        this.waitTimeSeconds = waitTimeSeconds;
        this.collector = collector;
        this.metrics = metrics;
    }

    @Override
    public void run() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsQueueUrl).withWaitTimeSeconds(waitTimeSeconds);
        try{
            while (run.get()) {
                ReceiveMessageResult result = client.receiveMessage(receiveMessageRequest);
                List<DeleteMessageBatchRequestEntry> deletes = new ArrayList<>();
                for (Message message : result.getMessages()) {
                    MessageAttributeValue value = message.getMessageAttributes().get("spans");
                    String type = message.getMessageAttributes().get("spans").getDataType();
                    switch (type.split(".")[1]) {
                        case "JSON":
                            collector.acceptSpans(value.getBinaryValue().array(), Codec.JSON, Callback.NOOP);
                            break;
                        case "THRIFT":
                            collector.acceptSpans(value.getBinaryValue().array(), Codec.THRIFT, Callback.NOOP);
                            break;
                    }
                    deletes.add(new DeleteMessageBatchRequestEntry(message.getMessageId(), message.getReceiptHandle()));
                }
                if (deletes.size() > 0) {
                    client.deleteMessageBatch(sqsQueueUrl, deletes);
                }
            }
        } finally {
            client.shutdown();
        }
    }

    @Override
    public void close() throws IOException {
        run.set(false);
    }
}
