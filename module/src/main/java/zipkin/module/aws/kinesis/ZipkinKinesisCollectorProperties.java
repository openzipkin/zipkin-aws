/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.kinesis;

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
