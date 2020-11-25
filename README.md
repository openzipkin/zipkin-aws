# zipkin-aws

[![Gitter chat](http://img.shields.io/badge/gitter-join%20chat%20%E2%86%92-brightgreen.svg)](https://gitter.im/openzipkin/zipkin)
[![Build Status](https://github.com/openzipkin/zipkin-aws/workflows/test/badge.svg)](https://github.com/openzipkin/zipkin-aws/actions?query=workflow%3Atest)
[![Maven Central](https://img.shields.io/maven-central/v/io.zipkin.aws/zipkin-module-aws.svg)](https://search.maven.org/search?q=g:io.zipkin.aws%20AND%20a:zipkin-module-aws)

Shared libraries that provide Zipkin integration with AWS Kinesis, SQS, 
and X-Ray. Requires JRE 8 or later.

# Usage
These components provide Zipkin 
[Reporters](https://github.com/openzipkin/zipkin-reporter-java/blob/master/core/src/main/java/zipkin2/reporter/Reporter.java) and 
[Senders](https://github.com/openzipkin/zipkin-reporter-java/blob/master/core/src/main/java/zipkin2/reporter/Sender.java),
which build off interfaces provided by the [zipkin-reporters-java](https://github.com/openzipkin/zipkin-reporter-java), and
and [Collectors](https://github.com/openzipkin/zipkin/blob/master/zipkin-collector/core/src/main/java/zipkin2/collector/Collector.java),
which are used in a [Zipkin](https://github.com/openzipkin/zipkin) server.

We also have tracing libraries that extend [Brave](https://github.com/openzipkin/brave).

## Tracing libraries
Tracing libraries extend [Brave](https://github.com/openzipkin/brave) and ensure Amazon libraries
are visible in your traces.

Instrumentation | Description
--- | ---
[AWS SDK](brave/instrumentation-aws-java-sdk-core) | Traces [AmazonWebServiceClient](https://github.com/aws/aws-sdk-java)
[AWS SDK V2](brave/instrumentation-aws-java-sdk-v2-core) | Traces [SdkClient](https://github.com/aws/aws-sdk-java-v2)
[AWS SQS Messaging](brave/instrumentation-aws-java-sdk-sqs) | Traces [AmazonSQS](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/AmazonSQS.html)

We also have a [library to read Amazon's trace header](brave/propagation-aws).

## Reporters and Senders
The component in a traced application that sends timing data (spans) out of process is called a
Reporter. It is responsible for handling the queueing and encoding of outbound spans.

Reporters that are sending Zipkin data to Zipkin typically make use of a Sender, which implements
the wire protocol to a particular technology used to send the encoded spans. Typically Senders are
called on interval by an [async reporter](https://github.com/openzipkin/zipkin-reporter-java#asyncreporter).

NOTE: Applications can be written in any language. While we currently only have Reporters and
Senders in Java, senders in other languages  are welcome.

Reporter | Description
--- | ---
[X-Ray UDP](reporter/reporter-xray-udp) | Reports spans to [X-Ray](https://aws.amazon.com/xray/), AWS's alternative to Zipkin.

Sender | Description
--- | ---
[SQS](reporter/sender-sqs) | Sends tracing data to Zipkin using [SQS](https://aws.amazon.com/sqs/), a message queue service.
[SQS v2](reporter/sender-awssdk-sqs) | Sends tracing data to Zipkin using [SQS](https://aws.amazon.com/sqs/), a message queue service.
[Kinesis](reporter/sender-kinesis) | Sends tracing data to Zipkin using [Kinesis](https://aws.amazon.com/kinesis/), an alternative similar to Kafka.

## Collectors
The component in a zipkin server that receives trace data is called a
collector. This decodes spans reported by applications and persists them
to a configured storage component.

Collector | Description
--- | ---
[SQS](collector/kinesis) | An alternative to Kafka.
[Kinesis](collector/kinesis) | An alternative to Kafka.

## Server integration

Integration with Zipkin server is done for you in [Docker][docker]. If you cannot use Docker, you
can integrate a [Java module instead](module).

Configuration layered over Zipkin server is documented [here](module).

## Artifacts
All artifacts publish to the group ID "io.zipkin.aws". We use a common release version for all
components.

### Library Releases
Releases are at [Sonatype](https://oss.sonatype.org/content/repositories/releases) and
[Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.zipkin.aws%22)

### Library Snapshots
Snapshots are uploaded to [Sonatype](https://oss.sonatype.org/content/repositories/snapshots) after
commits to master.

### Docker Images
Released versions of zipkin-aws are published to Docker Hub as `openzipkin/zipkin-aws`
and GitHub Container Registry as `ghcr.io/openzipkin/zipkin-aws`.

See [docker](docker) for details.