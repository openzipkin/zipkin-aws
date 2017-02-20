/**
 * Copyright 2016-2017 The OpenZipkin Authors
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

import static zipkin.internal.Util.checkNotNull;

public class KinesisStreamFactory implements StreamFactory {

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        String stream = "zipkin";
        String app = "zipkin-sparkstreaming";
        String awsRegion;
        String awsEndpoint;
        String awsAccessKeyId;
        String awsSecretKey;

        public Builder stream(String stream) {
            this.stream = stream;
            return this;
        }

        public Builder app(String app) {
            this.app = app;
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

        public Builder credentials(String awsAccessKeyId, String awsSecretKey) {
            checkNotNull(awsAccessKeyId, "Access Key/Secret Key pair");
            checkNotNull(awsSecretKey, "Access Key/Secret Key pair");
            this.awsAccessKeyId = awsAccessKeyId;
            this.awsSecretKey = awsSecretKey;
            return this;
        }

        public KinesisStreamFactory build() {
            return new KinesisStreamFactory(this);
        }
    }

    private final String stream;
    private final String app;
    private final String regionName;
    private final String endpoint;
    private String awsAccessKeyId;
    private String awsSecretKey;

    KinesisStreamFactory(Builder builder) {
        this.stream = builder.stream;
        this.app = builder.app;
        this.regionName = builder.awsRegion;
        this.endpoint = builder.awsEndpoint != null ?
                builder.awsEndpoint :
                Region.getRegion(Regions.valueOf(regionName)).getServiceEndpoint(AmazonKinesis.ENDPOINT_PREFIX);

        this.awsAccessKeyId = builder.awsAccessKeyId;
        this.awsSecretKey = builder.awsSecretKey;
    }

    @Override
    public JavaDStream<byte[]> create(JavaStreamingContext jsc) {
        if (awsAccessKeyId != null) {
            return KinesisUtils.createStream(
                    jsc,
                    stream,
                    app,
                    endpoint,
                    regionName,
                    InitialPositionInStream.TRIM_HORIZON, // TODO configurable?
                    new Duration(2000), // TODO configurable?
                    StorageLevel.MEMORY_AND_DISK_2(), // TODO configurable?
                    awsAccessKeyId,
                    awsSecretKey
            );
        }
        return KinesisUtils.createStream(
                jsc,
                stream,
                app,
                endpoint,
                regionName,
                InitialPositionInStream.TRIM_HORIZON, // TODO configurable?
                new Duration(2000), // TODO configurable?
                StorageLevel.MEMORY_AND_DISK_2() // TODO configurable?
        );
    }
}
