# autoconfigure-collector-sqs

## ZipkinSQSCollectorAutoConfiguration

This is a Spring Boot [AutoConfiguration](http://docs.spring.io/spring-boot/docs/current/reference/html/using-boot-auto-configuration.html) 
module that can be added to a [Zipkin Server](https://github.com/openzipkin/zipkin/tree/master/zipkin-server) 
deployment to collect Spans from Amazon SQS queues.  Internally
this module wraps the [SQSCollector](https://github.com/openzipkin/zipkin-aws/tree/master/collector-sqs) 
and exposes configuration options through environment variables.

## Quick start

JRE 8 is required to run Zipkin server.

Fetch the latest released
[executable jar for Zipkin server](https://search.maven.org/remote_content?g=org.apache.zipkin&a=zipkin-server&v=LATEST&c=exec)
and
[autoconfigure module jar for the sqs collector](https://search.maven.org/remote_content?g=io.zipkin.aws&a=zipkin-autoconfigure-collector-sqs&v=LATEST&c=module).
Run Zipkin server with the SQS collector enabled.

For example:

```bash
$ curl -sSL https://zipkin.apache.org/quickstart.sh | bash -s
$ curl -sSL https://zipkin.apache.org/quickstart.sh | bash -s io.zipkin.aws:zipkin-autoconfigure-collector-sqs:LATEST:module sqs.jar
$ SQS_QUEUE_URL=https://ap-southeast-1.queue.amazonaws.com/012345678901/zipkin \
      java \
      -Dloader.path='sqs.jar,sqs.jar!/lib' \
      -Dspring.profiles.active=sqs \
      -cp zipkin.jar \
      org.springframework.boot.loader.PropertiesLauncher
```

After executing these steps, applications can send spans
http://localhost:9411/api/v2/spans (or the legacy endpoint http://localhost:9411/api/v1/spans)

The Zipkin server can be further configured as described in the
[Zipkin server documentation](https://github.com/openzipkin/zipkin/blob/master/zipkin-server/README.md).

### Configuration

Configuration can be applied either through environment variables or an external Zipkin
configuration file.  The module includes default configuration that can be used as a 
[reference](https://github.com/openzipkin/zipkin-aws/tree/master/autoconfigure/collector-sqs/src/main/resources/zipkin-server-sqs.yml)
for users that prefer a file based approach.

##### Environment Variables

- `SQS_QUEUE_URL` The AWS SQS queue URL as provided in the AWS Console.
- `SQS_PARALLELISM` An integer that specifies how many collectors are used to poll SQS in parallel. 
_Default is 1_
- `SQS_WAIT_TIME_SECONDS` How long to wait for messages from SQS before making a new API call. 
_Default 20 seconds_
- `SQS_MAX_NUMBER_OF_MESSAGES` Max number of messages to accept for each SQS API call.
_Default 10_
- `SQS_AWS_ACCESS_KEY_ID` Optional AWS Access Key ID.
- `SQS_AWS_SECRET_ACCESS_KEY` Optional AWS Secret Access Key.
- `SQS_AWS_STS_ROLE_ARN` Optional IAM role ARN for cross account role delegation.
- `SQS_AWS_STS_REGION` Optional AWS region ID when using STS. _Default us-east-1_

### Security

The auto configuration library uses the AWS SQS Java SDK and follows credential
configuration resolution provided by the [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html).
In the case of using IAM role delegation the [STSAssumeRoleSessionCredentialsProvider](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/STSAssumeRoleSessionCredentialsProvider.html) 
is used.

The following IAM permissions are required by the SQSCollector

- sqs:ReceiveMessage
- sqs:DeleteMessageBatch

### Testing

Once your collector is enabled, verify it is running:
```bash
curl -s localhost:9411/health|jq .zipkin.details.SQSCollector
{
  "status": "UP"
}
```

Now, you can send a test message like below:
```bash
# send a json list with a single json-encoded span
$ aws sqs send-message --queue-url $SQS_QUEUE_URL --message-body '[{
  "traceId": "86154a4ba6e91385",
  "parentId": "86154a4ba6e91385",
  "id": "4d1e00c0db9010db",
  "kind": "CLIENT",
  "name": "get",
  "timestamp": 1472470996199000,
  "duration": 207000,
  "localEndpoint": {
    "serviceName": "frontend",
    "ipv4": "127.0.0.1"
  },
  "tags": {
    "http.path": "/api"
  }
}]'
# read it back using the zipkin v1 api
$ curl -s localhost:9411/api/v1/trace/86154a4ba6e91385|jq .[].traceId
"86154a4ba6e91385"
```
