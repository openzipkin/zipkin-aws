/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin.module.aws.sqs;

import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin2.collector.sqs.SQSCollector;

@ConfigurationProperties("zipkin.collector.sqs")
final class ZipkinSQSCollectorProperties {
  String queueUrl;
  int waitTimeSeconds = 20;
  int parallelism = 1;
  int maxNumberOfMessages = 10;
  String awsAccessKeyId;
  String awsSecretAccessKey;
  String awsStsRoleArn;
  String awsStsRegion = "us-east-1";

  public void setQueueUrl(String queueUrl) {
    this.queueUrl = queueUrl;
  }

  public String getQueueUrl() {
    return queueUrl;
  }

  public void setWaitTimeSeconds(int waitTimeSeconds) {
    this.waitTimeSeconds = waitTimeSeconds;
  }

  public int getWaitTimeSeconds() {
    return waitTimeSeconds;
  }

  public void setMaxNumberOfMessages(int maxNumberOfMessages) {
    this.maxNumberOfMessages = maxNumberOfMessages;
  }

  public int getMaxNumberOfMessages() {
    return this.maxNumberOfMessages;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setAwsAccessKeyId(String awsAccessKeyId) {
    this.awsAccessKeyId = awsAccessKeyId;
  }

  public String getAwsAccessKeyId() {
    return this.awsAccessKeyId;
  }

  public void setAwsSecretAccessKey(String awsSecretAccessKey) {
    this.awsSecretAccessKey = awsSecretAccessKey;
  }

  public String getAwsStsRegion() {
    return awsStsRegion;
  }

  public void setAwsStsRegion(String awsStsRegion) {
    this.awsStsRegion = awsStsRegion;
  }

  public String getAwsSecretAccessKey() {
    return this.awsSecretAccessKey;
  }

  public void setAwsStsRoleArn(String awsStsRoleArn) {
    this.awsStsRoleArn = awsStsRoleArn;
  }

  public String getAwsStsRoleArn() {
    return this.awsStsRoleArn;
  }

  public SQSCollector.Builder toBuilder() {
    return SQSCollector.newBuilder()
        .queueUrl(queueUrl)
        .parallelism(parallelism)
        .waitTimeSeconds(waitTimeSeconds)
        .maxNumberOfMessages(maxNumberOfMessages);
  }
}
