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
package zipkin.autoconfigure.collector.kinesis;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("zipkin.collector.kinesis")
class ZipkinKinesisCollectorProperties {
  static final String DEFAULT_AWS_REGION = "us-east-1";

  String streamName;
  String appName;

  String awsAccessKeyId;
  String awsSecretAccessKey;
  String awsStsRoleArn;
  String awsStsRegion;
  String awsKinesisRegion;
  String awsRegion = DEFAULT_AWS_REGION;

  public String getStreamName() {
    return streamName;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public String getAwsAccessKeyId() {
    return awsAccessKeyId;
  }

  public void setAwsAccessKeyId(String awsAccessKeyId) {
    this.awsAccessKeyId = awsAccessKeyId;
  }

  public String getAwsSecretAccessKey() {
    return awsSecretAccessKey;
  }

  public void setAwsSecretAccessKey(String awsSecretAccessKey) {
    this.awsSecretAccessKey = awsSecretAccessKey;
  }

  public String getAwsStsRoleArn() {
    return awsStsRoleArn;
  }

  public void setAwsStsRoleArn(String awsStsRoleArn) {
    this.awsStsRoleArn = awsStsRoleArn;
  }

  public String getAwsStsRegion() {
    return awsStsRegion;
  }

  public void setAwsStsRegion(String awsStsRegion) {
    this.awsStsRegion = awsStsRegion;
  }

  public String getAwsKinesisRegion() {
    return awsKinesisRegion;
  }

  public void setAwsKinesisRegion(String awsKinesisRegion) {
    this.awsKinesisRegion = awsKinesisRegion;
  }

  public String getAwsRegion() {
    return awsRegion;
  }

  public void setAwsRegion(String awsRegion) {
    this.awsRegion = awsRegion;
  }
}
