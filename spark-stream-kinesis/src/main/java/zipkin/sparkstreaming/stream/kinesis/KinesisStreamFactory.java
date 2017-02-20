package zipkin.sparkstreaming.stream.kinesis;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import zipkin.sparkstreaming.StreamFactory;

public class KinesisStreamFactory implements StreamFactory {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        String streamName = "zipkin";
        String kinesisAppName = "zipkin-sparkstreaming";
        String awsRegion;
        String awsEndpoint;

        public Builder streamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        public Builder kinesisAppName(String kinesisAppName) {
            this.kinesisAppName = kinesisAppName;
            return this;
        }

        public Builder awsRegion(String awsRegion) {
            this.awsRegion = awsRegion;
            return this;
        }

        public Builder awsEndpoint(String awsEndpoint) {
            this.awsEndpoint = awsEndpoint;
            return this;
        }

        public KinesisStreamFactory build() {
            return new KinesisStreamFactory(this);
        }
    }

    private final String streamName;
    private final String kinesisAppName;
    private final String regionName;
    private final String endpoint;

    KinesisStreamFactory(Builder builder) {
        this.streamName = builder.streamName;
        this.kinesisAppName = builder.kinesisAppName;
        this.regionName = builder.awsRegion;
        this.endpoint = builder.awsEndpoint != null ?
                builder.awsEndpoint :
                Region.getRegion(Regions.valueOf(regionName)).getServiceEndpoint(AmazonKinesis.ENDPOINT_PREFIX);
    }

    @Override
    public JavaDStream<byte[]> create(JavaStreamingContext jsc) {
        return KinesisUtils.createStream(
                jsc,
                kinesisAppName,
                streamName,
                endpoint,
                regionName,
                InitialPositionInStream.TRIM_HORIZON, // TODO configurable?
                new Duration(2000), // TODO configurable?
                StorageLevel.MEMORY_AND_DISK_2() // TODO configurable?
        );
    }
}
