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
package zipkin.autoconfigure.collector.sqs;

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
