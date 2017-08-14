# autoconfigure-collector-kinesis

## ZipkinKinesisCollectorAutoConfiguration

This is a Spring Boot [AutoConfiguration](http://docs.spring.io/spring-boot/docs/current/reference/html/using-boot-auto-configuration.html) 
module that can be added to a [Zipkin Server](https://github.com/openzipkin/zipkin/tree/master/zipkin-server) 
deployment to collect Spans from Amazon Kinesis streams.  Internally
this module wraps the [KinesisCollector](https://github.com/openzipkin/zipkin-aws/tree/master/collector-kinesis) 
and exposes configuration options through environment variables.

## Usage

Download the module from [TODO] link and extract it to a directory relative to the
Zipkin Server jar.

### Configuration

Configuration can be applied either through environment variables or an external Zipkin
configuration file.  The module includes default configuration that can be used as a 
[reference](https://github.com/openzipkin/zipkin-aws/tree/master/autoconfigure/collector-kinesis/src/main/resources/zipkin-server-kinesis.yml)
for users that prefer a file based approach.

##### Environment Variables

- `KINESIS_STREAM_NAME` The name of the Kinesis stream to read from
- `KINESIS_APP_NAME` The name for this app to use for sharing a stream _Defaults to zipkin_
- `KINESIS_AWS_ACCESS_KEY_ID` Optional AWS Access Key ID.
- `KINESIS_AWS_SECRET_ACCESS_KEY` Optional AWS Secret Access Key.
- `KINESIS_AWS_STS_ROLE_ARN` Optional IAM role ARN for cross account role delegation.
- `KINESIS_AWS_STS_REGION` Optional AWS region ID when using STS. _Default us-east-1_

### Running

```bash
KINESIS_STREAM_NAME="zipkin-spans"
KINESIS_APP_NAME="zipkin-server"
KINESIS_AWS_ACCESS_KEY_ID="XqgzeGF3tC7u"
KINESIS_AWS_SECRET_ACCESS_KEY="F75uEjHM7ykLzXDJMTHrQ5Jr"
java -Dloader.path=kinesis -Dspring.profiles.active=kinesis -cp zipkin.jar org.springframework.boot.loader.PropertiesLauncher
```

### Security

The auto configuration library uses the AWS Kinesis Java SDK and follows credential
configuration resolution provided by the [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html).
In the case of using IAM role delegation the [STSAssumeRoleSessionCredentialsProvider](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/STSAssumeRoleSessionCredentialsProvider.html) 
is used.

The following IAM permissions are required by the KinesisCollector

- kinesis:DescribeStream
- kinesis:GetRecords
- kinesis:GetShardIterator
- dynamodb:CreateTable
- dynamodb:DescribeTable
- dynamodb:GetItem
- dynamodb:PutItem
- dynamodb:Scan
- dynamodb:UpdateItem
- dynamodb:DeleteItem
- cloudwatch:PutMetricData

### Testing

Once your collector is enabled, verify it is running:
```bash
$ curl -s localhost:9411/health|jq .zipkin.KinesisCollector
{
  "status": "UP"
}
```

Now, you can send a test message like below:
```bash
# send a json list with a single json-encoded span
$ aws kinesis put-record --stream-name $KINESIS_STREAM_NAME --partition-key $(hostname) --data '[{
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