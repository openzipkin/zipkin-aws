package zipkin.collector.sqs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sqs.AmazonSQSClient;
import zipkin.collector.Collector;
import zipkin.collector.CollectorComponent;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.internal.Util;
import zipkin.storage.StorageComponent;

import java.io.Closeable;
import java.io.IOException;

public class SqsCollector implements CollectorComponent, Closeable {

    private Collector collector;
    private CollectorMetrics metrics;

    private AWSCredentialsProvider credentialsProvider;
    private String sqsQueueUrl;
    private int waitTimeSeconds;

    private AmazonSQSClient client;

    public SqsCollector(Builder builder) {
        collector = builder.delegate.build();
        metrics = builder.metrics;
        credentialsProvider = builder.credentialsProvider;
        sqsQueueUrl = builder.sqsQueueUrl;
        waitTimeSeconds = builder.waitTimeSeconds;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder implements CollectorComponent.Builder {
        Collector.Builder delegate = Collector.builder(SqsCollector.class);
        CollectorMetrics metrics = CollectorMetrics.NOOP_METRICS;

        String sqsQueueUrl;
        int waitTimeSeconds = 1;
        AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

        @Override
        public CollectorComponent.Builder storage(StorageComponent storage) {
            delegate.storage(storage);
            return this;
        }

        @Override
        public CollectorComponent.Builder metrics(CollectorMetrics metrics) {
            this.metrics = Util.checkNotNull(metrics, "metrics").forTransport("kafka");
            delegate.metrics(metrics);
            return this;
        }

        @Override
        public CollectorComponent.Builder sampler(CollectorSampler sampler) {
            delegate.sampler(sampler);
            return this;
        }

        public Builder sqsQueueUrl(String sqsQueueUrl) {
            this.sqsQueueUrl = sqsQueueUrl;
            return this;
        }

        public Builder awsCredentialsProvider(AWSCredentialsProvider awsCredentialsProvider) {
            this.credentialsProvider = awsCredentialsProvider;
            return this;
        }

        public Builder waitTimeSeconds(int waitTimeSeconds) {
            this.waitTimeSeconds = waitTimeSeconds;
            return this;
        }

        @Override
        public SqsCollector build() {
            return new SqsCollector(this);
        }

        Builder() {
        }
    }

    @Override
    public CollectorComponent start() {
        client = new AmazonSQSClient(credentialsProvider);

        new Thread(new SqsStreamProcessor(client, sqsQueueUrl, waitTimeSeconds, collector, metrics)).run();

        return this;
    }

    @Override
    public CheckResult check() {
        return CheckResult.OK;
    }

    @Override
    public void close() throws IOException {
        client.shutdown();
    }
}
