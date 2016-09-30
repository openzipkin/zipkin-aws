package zipkin.collector.sqs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sqs.AmazonSQSClient;
import zipkin.collector.Collector;
import zipkin.collector.CollectorComponent;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.CollectorSampler;
import zipkin.internal.LazyCloseable;
import zipkin.storage.StorageComponent;

import java.io.Closeable;
import java.io.IOException;

public class SqsCollector implements CollectorComponent, Closeable {

    private Collector collector;
    private AWSCredentialsProvider credentialsProvider;
    private String sqsQueueUrl;

    private AmazonSQSClient client;

    public SqsCollector(Builder builder) {
        collector = builder.delegate.build();
        credentialsProvider = builder.credentialsProvider;
        sqsQueueUrl = builder.sqsQueueUrl;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder implements CollectorComponent.Builder {
        Collector.Builder delegate = Collector.builder(SqsCollector.class);

        String sqsQueueUrl;
        AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

        @Override
        public CollectorComponent.Builder storage(StorageComponent storage) {
            delegate.storage(storage);
            return this;
        }

        @Override
        public CollectorComponent.Builder metrics(CollectorMetrics metrics) {
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

        // Do the collection

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
