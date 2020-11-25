# module-aws

## Overview

This is a module that can be added to a [Zipkin Server](https://github.com/openzipkin/zipkin/tree/master/zipkin-server)
deployment to add Amazon Web Services based collection and storage features.

## Quick start

The easiest way is to use our [../docker][Docker image]. Please prefer this to building your own
distribution.

JRE 8 is required to run Zipkin server.

Fetch the latest released
[executable jar for Zipkin server](https://search.maven.org/remote_content?g=io.zipkin&a=zipkin-server&v=LATEST&c=exec)
and
[module jar for AWS](https://search.maven.org/remote_content?g=io.zipkin.aws&a=zipkin-module-aws&v=LATEST&c=module).
Run Zipkin server with the AWS module enabled.

For example, to use AWS SQS:

```bash
$ curl -sSL https://zipkin.io/quickstart.sh | bash -s
$ curl -sSL https://zipkin.io/quickstart.sh | bash -s io.zipkin.aws:zipkin-module-aws:LATEST:module sqs.jar
$ SQS_QUEUE_URL=https://ap-southeast-1.queue.amazonaws.com/012345678901/zipkin \
  java -Dloader.path='aws.jar,aws.jar!/lib' -Dspring.profiles.active=aws \
       -cp zipkin.jar org.springframework.boot.loader.PropertiesLauncher
```

*Note:* By default, this module will search for credentials in the $HOME/.aws directory.

After executing these steps, applications can send spans
http://localhost:9411/api/v2/spans (or the legacy endpoint http://localhost:9411/api/v1/spans)

The Zipkin server can be further configured as described in the
[Zipkin server documentation](https://github.com/openzipkin/zipkin/blob/master/zipkin-server/README.md).

### Configuration

Configuration can be applied either through environment variables or an external Zipkin
configuration file.  The module includes default configuration that can be used as a 
[reference](https://github.com/openzipkin/zipkin-aws/tree/master/module/src/main/resources/zipkin-server-aws.yml)
for users that prefer a file based approach.

The following variables apply to multiple, if not all options:

Environment Variable | Description
--- | ---
`AWS_REGION` | Optional AWS Region, implicitly sets STS and Kinesis regions if not provided, defaults to us-east-1

*Note:* By default, this module will search for credentials in the $HOME/.aws directory.

### Kinesis Collector
[Kinesis Collector](../collector/kinesis) is enabled when `KINESIS_STREAM_NAME` is set. The
following settings apply in this case.

Environment Variable | Property | Description
--- | --- | ---
`KINESIS_STREAM_NAME` | `zipkin.collector.kinesis.stream-name` | The name of the Kinesis stream to read from
`KINESIS_APP_NAME` | `zipkin.collector.kinesis.app-name` | The name for this app to use for sharing a stream. Defaults to `zipkin`
`KINESIS_AWS_ACCESS_KEY_ID` | `zipkin.collector.kinesis.aws-access-key-id` | Optional AWS Access Key
`KINESIS_AWS_SECRET_ACCESS_KEY` | `zipkin.collector.kinesis.aws-secret-access-key` | Optional AWS Secret Access Key
`KINESIS_AWS_REGION` | `zipkin.collector.kinesis.aws-kinesis-region` | Optional AWS Kinesis Region. Defaults to `AWS_REGION`
`KINESIS_AWS_STS_ROLE_ARN` | `zipkin.collector.kinesis.aws-sts-role-arn` | Optional IAM role ARN for cross account role delegation
`KINESIS_AWS_STS_REGION` | `zipkin.collector.kinesis.aws-kinesis-region` | Optional AWS region ID when using STS. Defaults to `KINESIS_AWS_REGION`

Example usage:

```bash
$ KINESIS_STREAM_NAME=zipkin \
  java -Dloader.path='aws.jar,aws.jar!/lib' -Dspring.profiles.active=aws \
    -cp zipkin.jar org.springframework.boot.loader.PropertiesLauncher
```

#### Security

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

#### Testing

Once your collector is enabled, verify it is running:
```bash
$ curl -s localhost:9411/health|jq .zipkin.details.KinesisCollector
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

### SQS Collector
[SQS Collector](../collector/sqs) is enabled when `SQS_QUEUE_URL` is set. The following settings
apply in this case.

Environment Variable | Property | Description
--- | --- | ---
`SQS_QUEUE_URL` | `zipkin.collector.sqs.queue-url` | The AWS SQS queue URL as provided in the AWS Console
`SQS_PARALLELISM` | `zipkin.collector.sqs.parallelism` | The count of collectors that poll SQS in parallel. Defaults to 1
`SQS_WAIT_TIME_SECONDS` | `zipkin.collector.sqs.wait-time-seconds` | How long to wait for messages from SQS before making a new API call. Defaults to 20
`SQS_MAX_NUMBER_OF_MESSAGES` | `zipkin.collector.sqs.max-number-of-messages` | Max number of messages to accept for each SQS API call. Defaults to 10
`SQS_AWS_ACCESS_KEY_ID` | `zipkin.collector.sqs.aws-access-key-id` | Optional AWS Access Key
`SQS_AWS_SECRET_ACCESS_KEY` | `zipkin.collector.sqs.aws-secret-access-key` | Optional AWS Secret Access Key
`SQS_AWS_STS_ROLE_ARN` | `zipkin.collector.sqs.aws-sts-role-arn` | Optional IAM role ARN for cross account role delegation
`SQS_AWS_STS_REGION` | `zipkin.collector.sqs.aws-sqs-region` | Optional AWS region ID when using STS. Defaults to `AWS_REGION`

Example usage:

```bash
$ SQS_QUEUE_URL=https://ap-southeast-1.queue.amazonaws.com/012345678901/zipkin \
  java -Dloader.path='aws.jar,aws.jar!/lib' -Dspring.profiles.active=aws \
    -cp zipkin.jar org.springframework.boot.loader.PropertiesLauncher
```

#### Security

The auto configuration library uses the AWS SQS Java SDK and follows credential
configuration resolution provided by the [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html).
In the case of using IAM role delegation the [STSAssumeRoleSessionCredentialsProvider](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/STSAssumeRoleSessionCredentialsProvider.html) 
is used.

The following IAM permissions are required by the SQSCollector

- sqs:ReceiveMessage
- sqs:DeleteMessageBatch

#### Testing

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

### Elasticsearch Storage
[Amazon Elasticsearch Service](../storage/elasticsearch) is enabled when
`STORAGE_TYPE=elasticsearch` and `ES_HOSTS` includes an Amazon Web Services hostname or
`ES_AWS_DOMAIN` is set. The following settings apply in this case.

Environment Variable | Property | Description
--- | --- | ---
`ES_HOSTS` | `zipkin.storage.elasticsearch.hosts` | An AWS-hosted elasticsearch installation (e.g. https://search-domain-xyzzy.us-west-2.es.amazonaws.com)
`ES_AWS_DOMAIN` | `zipkin.storage.elasticsearch.domain` | The name of the AWS-hosted elasticsearch domain to use. Supercedes any `ES_HOSTS`.
`ES_AWS_REGION` | `zipkin.storage.elasticsearch.aws.region` | Optional AWS region ID when looking up `ES_AWS_DOMAIN`. Defaults to `AWS_REGION`

*Note:* Zipkin will sign outbound requests to the cluster.

Example usage:

```bash
$ STORAGE_TYPE=elasticsearch ES_HOSTS=https://search-mydomain-2rlih66ibw43ftlk4342ceeewu.ap-southeast-1.es.amazonaws.com \
  java -Dloader.path='aws.jar,aws.jar!/lib' -Dspring.profiles.active=aws \
    -cp zipkin.jar org.springframework.boot.loader.PropertiesLauncher
```

Alternatively, you can have zipkin implicitly lookup your domain's URL:
```bash
$ STORAGE_TYPE=elasticsearch ES_AWS_DOMAIN=mydomain ES_AWS_REGION=ap-southeast-1 \
  java -Dloader.path='aws.jar,aws.jar!/lib' -Dspring.profiles.active=aws \
    -cp zipkin.jar org.springframework.boot.loader.PropertiesLauncher
```

#### Security

Before you start, make sure your CLI credentials are setup as Zipkin will read them:
```bash
$ aws es describe-elasticsearch-domain --domain-name mydomain|jq .DomainStatus.Endpoint
"search-mydomain-2rlih66ibw43ftlk4342ceeewu.ap-southeast-1.es.amazonaws.com"
```

`ES_AWS_DOMAIN` triggers the same request signing behavior as with `ES_HOSTS`, but requires
additional IAM permission to describe the given domain.

#### Testing

Once your storage is enabled, verify it is running:
```bash
$ curl -s localhost:9411/health|jq .zipkin.details.ElasticsearchStorage
{
  "status": "UP"
}
```

### XRay Storage
[Amazon XRay Storage](../storage/xray) is enabled when `STORAGE_TYPE=xray`. The following settings
apply in this case.

Environment Variable | Property | Description
--- | --- | ---
`AWS_XRAY_DAEMON_ADDRESS` | `zipkin.storage.xray.daemon-address` | Amazon X-Ray Daemon UDP daemon address. Defaults to `localhost:2000`

Example usage:

```bash
$ STORAGE_TYPE=xray java -Dloader.path='aws.jar,aws.jar!/lib' -Dspring.profiles.active=aws \
    -cp zipkin.jar org.springframework.boot.loader.PropertiesLauncher
```

#### Experimental
* **This is currently experimental!**
* This currently only supports sending to an XRay UDP daemon, not reading back spans from the service. 
* This requires reporters send 128-bit trace IDs, with the first 32bits as epoch seconds
* Check https://github.com/openzipkin/b3-propagation/issues/6 for tracers that support epoch128 trace IDs

#### Testing

Once your storage is enabled, verify it is running:
```bash
$ curl -s localhost:9411/health|jq .zipkin.details.XRayUDPStorage
{
  "status": "UP"
}
```
