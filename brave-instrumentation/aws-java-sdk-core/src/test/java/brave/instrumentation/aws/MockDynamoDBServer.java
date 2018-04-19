package brave.instrumentation.aws;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class MockDynamoDBServer implements TestRule {
  private final MockWebServer delegate = new MockWebServer();

  public String url() {
    return "http://localhost:" + delegate.getPort();
  }

  void enqueue(MockResponse mockResponse) {
    delegate.enqueue(mockResponse);
  }

  @Override public Statement apply(Statement statement, Description description) {
    return delegate.apply(statement, description);
  }
}
