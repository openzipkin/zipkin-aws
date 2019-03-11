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
