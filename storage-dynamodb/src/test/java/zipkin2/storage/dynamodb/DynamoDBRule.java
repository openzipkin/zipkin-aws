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
import org.junit.rules.ExternalResource;
import org.testcontainers.containers.GenericContainer;

public class DynamoDBRule extends ExternalResource {

  // Run test container: `docker run -p 8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb`
  private GenericContainer dynamoDBLocal = new GenericContainer<>("amazon/dynamodb-local")
      .withExposedPorts(8000)
      .withCommand("-jar DynamoDBLocal.jar -sharedDb");

  private AmazonDynamoDBAsync dynamoDB;

  private ZipkinSpansTable zipkinSpansTable;
  private ZipkinServiceSpanNamesTable zipkinServiceSpanNamesTable;
  private ZipkinAutocompleteTagsTable zipkinAutocompleteTagsTable;
  private ZipkinDependenciesTable zipkinDependenciesTable;

  @Override protected void before() throws Throwable {
    dynamoDBLocal.start();

    dynamoDB = AmazonDynamoDBAsyncClientBuilder.standard()
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(String.format("http://%s:%d", dynamoDBLocal.getContainerIpAddress(), dynamoDBLocal.getFirstMappedPort()), "us-east-1"))
        .withCredentials(
            new AWSStaticCredentialsProvider(new BasicAWSCredentials("access", "secret")))
        .withClientConfiguration(new ClientConfiguration()
            .withRetryPolicy(
                new RetryPolicy(RetryPolicy.RetryCondition.NO_RETRY_CONDITION, null, 0, true))
            .withConnectionTimeout(50))
        .build();

    zipkinSpansTable = new ZipkinSpansTable(dynamoDB);
    zipkinServiceSpanNamesTable = new ZipkinServiceSpanNamesTable(dynamoDB);
    zipkinAutocompleteTagsTable = new ZipkinAutocompleteTagsTable(dynamoDB);
    zipkinDependenciesTable = new ZipkinDependenciesTable(dynamoDB);

    createTables();
  }

  @Override protected void after() {
    dynamoDBLocal.stop();
  }

  public AmazonDynamoDBAsync dynamoDB() {
    return dynamoDB;
  }

  public void cleanUp() {
    dropTables();
    createTables();
  }

  private void createTables() {
    zipkinSpansTable.create();
    zipkinServiceSpanNamesTable.create();
    zipkinAutocompleteTagsTable.create();
    zipkinDependenciesTable.create();
  }

  private void dropTables() {
    zipkinSpansTable.drop();
    zipkinServiceSpanNamesTable.drop();
    zipkinAutocompleteTagsTable.drop();
    zipkinDependenciesTable.drop();
  }
}
