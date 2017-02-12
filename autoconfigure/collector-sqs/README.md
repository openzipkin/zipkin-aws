# autoconfigure-collector-sqs

## ZipkinSQSCollectorAutoConfiguration

This is a Spring Boot [AutoConfiguration](http://docs.spring.io/spring-boot/docs/current/reference/html/using-boot-auto-configuration.html) 
module that can be added to a [Zipkin Server](https://github.com/openzipkin/zipkin/tree/master/zipkin-server) 
deployment to collect Spans from Amazon SQS queues.  Internally
this module wraps the [SQSCollector](https://github.com/openzipkin/zipkin-aws/tree/master/collector-sqs) 
and exposes configuration options through environment variables.

## Usage

Download the module from [TODO] link and extract it to a directory relative to the
Zipkin Server jar.

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

### Running

```bash
SQS_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/123456789/my-zipkin-queue"
SQS_PARALLELISM=10
SQS_AWS_ACCESS_KEY_ID="XqgzeGF3tC7u"
SQS_AWS_SECRET_ACCESS_KEY="F75uEjHM7ykLzXDJMTHrQ5Jr"
java -Dloader.path=sqs -Dspring.profiles.active=sqs -cp zipkin.jar org.springframework.boot.loader.PropertiesLauncher
```

### Security

The auto configuration library uses the AWS SQS Java SDK and follows credential
configuration resolution provided by the [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html).
In the case of using IAM role delegation the [STSAssumeRoleSessionCredentialsProvider](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/STSAssumeRoleSessionCredentialsProvider.html) 
is used.

The following IAM permissions are required by the SQSCollector

- sqs:ReceiveMessage
- sqs:DeleteMessageBatch
