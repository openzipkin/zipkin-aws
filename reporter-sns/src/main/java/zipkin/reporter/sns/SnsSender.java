package zipkin.reporter.sns;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.util.Base64;
import com.google.auto.value.AutoValue;
import zipkin.internal.LazyCloseable;
import zipkin.reporter.BytesMessageEncoder;
import zipkin.reporter.Callback;
import zipkin.reporter.Encoding;
import zipkin.reporter.Sender;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

@AutoValue
public abstract class SnsSender extends LazyCloseable<AmazonSNSClient> implements Sender {
    abstract String topicArn();
    abstract AWSCredentialsProvider awsCredentialsProvider();
    public abstract Encoding encoding();

    public static Builder builder() {
        return new AutoValue_SnsSender.Builder()
                .awsCredentialsProvider(new DefaultAWSCredentialsProviderChain())
                .encoding(Encoding.JSON);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder topicArn(String topicArn);

        public abstract Builder awsCredentialsProvider(AWSCredentialsProvider awsCredentialsProvider);

        public abstract Builder encoding(Encoding encoding);

        public abstract SnsSender build();
    }

    public int messageMaxBytes() {
        return (256 * 1024) - 512; // Leave some slack for message attributes
    }

    public int messageSizeInBytes(List<byte[]> list) {
        return encoding().listSizeInBytes(list);
    }

    public void sendSpans(List<byte[]> list, Callback callback) {
        PublishRequest publishRequest = new PublishRequest(topicArn(), "z");
        publishRequest.addMessageAttributesEntry(
                "spans",
                new MessageAttributeValue()
                        .withDataType("Binary." + encoding().name())
                        .withBinaryValue(ByteBuffer.wrap(BytesMessageEncoder.forEncoding(encoding()).encode(list))));
        get().publish(publishRequest);
    }

    public CheckResult check() {
        return CheckResult.OK;
    }

    public void close() throws IOException {
        get().shutdown();
    }

    @Override
    protected AmazonSNSClient compute() {
        return new AmazonSNSClient(awsCredentialsProvider());
    }
}
