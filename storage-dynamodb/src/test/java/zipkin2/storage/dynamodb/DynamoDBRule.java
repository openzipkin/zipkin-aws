/*
 * Copyright 2016-2019 The OpenZipkin Authors
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
package zipkin2.storage.dynamodb;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.junit.AssumptionViolatedException;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import zipkin2.CheckResult;

final class DynamoDBRule extends ExternalResource {
  static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBRule.class);
  static final int DYNAMODB_PORT = 8000;
  final String image;
  GenericContainer container;
  DynamoDBStorage storage;

  DynamoDBRule(String image) {
    this.image = image;
  }

  @Override protected void before() {
    try {
      LOGGER.info("Starting docker image " + image);
      // Run test container: `docker run -p 8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb`
      container = new GenericContainer(image)
          .withExposedPorts(DYNAMODB_PORT)
          .withCommand("-jar DynamoDBLocal.jar -sharedDb")
          .withLogConsumer(new Consumer<OutputFrame>() {
            @Override public void accept(OutputFrame outputFrame) {
              System.out.println(outputFrame.getUtf8String());
            }
          })
          .waitingFor(new HostPortWaitStrategy());
      container.start();
      System.out.println("Starting docker image " + image);
    } catch (RuntimeException e) {
      LOGGER.warn("Couldn't start docker image " + image + ": " + e.getMessage(), e);
    }

    tryToInitializeClient();
  }

  void tryToInitializeClient() {
    DynamoDBStorage result = computeStorageBuilder().build();
    CheckResult check = result.check();
    if (!check.ok()) {
      throw new AssumptionViolatedException(check.error().getMessage(), check.error());
    }
    this.storage = result;
  }

  DynamoDBStorage.Builder computeStorageBuilder() {
    if (storage != null) {
      storage.close();
      ((ExecutorService) storage.executor).shutdownNow();
    }
    AmazonDynamoDBAsync client = AmazonDynamoDBAsyncClientBuilder.standard()
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(
                String.format("http://%s:%d", container.getContainerIpAddress(),
                    container.getFirstMappedPort()), "us-east-1"))
        .withCredentials(
            new AWSStaticCredentialsProvider(new BasicAWSCredentials("access", "secret")))
        .withClientConfiguration(new ClientConfiguration()
            .withRetryPolicy(
                new RetryPolicy(RetryPolicy.RetryCondition.NO_RETRY_CONDITION, null, 0, true))
            .withConnectionTimeout(50))
        .build();
    // TODO: schema creation should be built-in like elasticsearch and cassandra (ensureSchema)
    new ZipkinSpansTable(client).create();
    new ZipkinSearchValuesTable(client).create();
    new ZipkinDependenciesTable(client).create();
    return DynamoDBStorage.newBuilder(client).executor(Executors.newSingleThreadExecutor());
  }

  void clear() {
    if (storage == null) return;
    new ZipkinSpansTable(storage.client).truncate();
    new ZipkinSearchValuesTable(storage.client).truncate();
    new ZipkinDependenciesTable(storage.client).truncate();
  }

  @Override protected void after() {
    if (storage != null) {
      storage.close();
      ((ExecutorService) storage.executor).shutdownNow();
    }
    if (container != null) {
      LOGGER.info("Stopping docker image " + image);
      container.stop();
    }
  }
}
