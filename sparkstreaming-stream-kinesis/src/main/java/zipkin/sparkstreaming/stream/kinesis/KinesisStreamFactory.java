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

public class KinesisStreamFactory implements StreamFactory {

  public static Builder newBuilder() {
    return new Builder()
        .checkpointIntervalMillis(2000)
        .initialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2());
  }

  public static final class Builder {
    String stream = "zipkin";
    String app = "zipkin-sparkstreaming";
    String awsRegion;
    String awsEndpoint;
    Duration checkpointInterval;
    InitialPositionInStream initialPositionInStream;
    StorageLevel storageLevel;
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

    public Builder checkpointInterval(Duration checkpointInterval) {
      this.checkpointInterval = checkpointInterval;
      return this;
    }

    public Builder checkpointIntervalMillis(int checkpointIntervalMillis) {
      this.checkpointInterval = new Duration(checkpointIntervalMillis);
      return this;
    }

    public Builder initialPositionInStream(InitialPositionInStream initialPositionInStream) {
      this.initialPositionInStream = initialPositionInStream;
      return this;
    }

    public Builder storageLevel(StorageLevel storageLevel) {
      this.storageLevel = storageLevel;
      return this;
    }

    public Builder credentials(String awsAccessKeyId, String awsSecretKey) {
      if (awsAccessKeyId == null) throw new NullPointerException("awsAccessKeyId == null");
      if (awsSecretKey == null) throw new NullPointerException("awsSecretKey == null");
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
  private final Duration checkpointInterval;
  private final InitialPositionInStream initialPositionInStream;
  private final StorageLevel storageLevel;
  private String awsAccessKeyId;
  private String awsSecretKey;

  KinesisStreamFactory(Builder builder) {
    this.stream = builder.stream;
    this.app = builder.app;
    this.regionName = builder.awsRegion;
    this.endpoint = builder.awsEndpoint != null ?
        builder.awsEndpoint :
        Region.getRegion(Regions.fromName(regionName))
            .getServiceEndpoint(AmazonKinesis.ENDPOINT_PREFIX);

    this.checkpointInterval = builder.checkpointInterval;
    this.initialPositionInStream = builder.initialPositionInStream;
    this.storageLevel = builder.storageLevel;

    this.awsAccessKeyId = builder.awsAccessKeyId;
    this.awsSecretKey = builder.awsSecretKey;
  }

  @Override
  public JavaDStream<byte[]> create(JavaStreamingContext jsc) {
    if (awsAccessKeyId != null) {
      return KinesisUtils.createStream(
          jsc,
          app,
          stream,
          endpoint,
          regionName,
          initialPositionInStream,
          checkpointInterval,
          storageLevel,
          awsAccessKeyId,
          awsSecretKey
      );
    }
    return KinesisUtils.createStream(
        jsc,
        app,
        stream,
        endpoint,
        regionName,
        initialPositionInStream,
        checkpointInterval,
        storageLevel
    );
  }
}
