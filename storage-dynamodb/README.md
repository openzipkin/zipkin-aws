# zipkin-storage-dynamodb

## Testing
This module conditionally runs integration tests against a DynamoDB container.

If you run tests via Maven or otherwise when the docker container doesn't start, you'll notice tests are silently skipped.
```
Results :

Tests run: 62, Failures: 0, Errors: 0, Skipped: 48
```

This behaviour is intentional: We don't want to burden developers with
installing and running all storage options to test unrelated change.
That said, all integration tests run on pull request via Travis.

### Running a single test

To run a single integration test, use the following syntax:

```bash
$ ./mvnw -Dit.test='ITDynamoDBStorage$ITSearchEnabledFalse#getSpanNames_isEmpty' -pl storage-dynamodb clean verify
```

