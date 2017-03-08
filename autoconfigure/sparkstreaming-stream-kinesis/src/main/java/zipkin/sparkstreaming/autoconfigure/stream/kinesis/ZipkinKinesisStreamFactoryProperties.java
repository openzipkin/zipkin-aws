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
package zipkin.sparkstreaming.autoconfigure.stream.kinesis;

import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin.sparkstreaming.stream.kinesis.KinesisStreamFactory;

@ConfigurationProperties(ZipkinKinesisStreamFactoryProperties.PROPERTIES_ROOT)
public class ZipkinKinesisStreamFactoryProperties {
    public static final String PROPERTIES_ROOT = "zipkin.sparkstreaming.stream.kinesis";

    private String kinesisStream;
    private String app;
    private String region;
    private String kinesisEndpoint;
    private int checkpointIntervalMillis = 0;
    private String awsAccessKeyId;
    private String awsSecretKey;

    public String getKinesisStream() {
        return kinesisStream;
    }

    public void setKinesisStream(String kinesisStream) {
        this.kinesisStream = kinesisStream;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getKinesisEndpoint() {
        return kinesisEndpoint;
    }

    public void setKinesisEndpoint(String kinesisEndpoint) {
        this.kinesisEndpoint = kinesisEndpoint;
    }

    public int getCheckpointIntervalMillis() {
        return checkpointIntervalMillis;
    }

    public void setCheckpointIntervalMillis(int checkpointIntervalMillis) {
        this.checkpointIntervalMillis = checkpointIntervalMillis;
    }

    public String getAwsAccessKeyId() {
        return awsAccessKeyId;
    }

    public void setAwsAccessKeyId(String awsAccessKeyId) {
        this.awsAccessKeyId = awsAccessKeyId;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public void setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
    }

    KinesisStreamFactory.Builder toBuilder() {
        KinesisStreamFactory.Builder builder = KinesisStreamFactory.newBuilder()
                .awsRegion(region);

        if (kinesisStream != null) builder.stream(kinesisStream);
        if (app != null) builder.app(app);
        if (kinesisEndpoint != null) builder.awsEndpoint(kinesisEndpoint);
        if (checkpointIntervalMillis > 0) builder.checkpointIntervalMillis(checkpointIntervalMillis);
        if (awsAccessKeyId != null && awsSecretKey != null) builder.credentials(awsAccessKeyId, awsSecretKey);

        return builder;
    }

}
