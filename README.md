[![Gitter chat](http://img.shields.io/badge/gitter-join%20chat%20%E2%86%92-brightgreen.svg)](https://gitter.im/openzipkin/zipkin)
[![Build Status](https://travis-ci.org/openzipkin/zipkin-aws.svg?branch=master)](https://travis-ci.org/openzipkin/zipkin-aws)
[![Maven Central](https://img.shields.io/maven-central/v/io.zipkin.aws.svg)](https://search.maven.org/search?q=g:io.zipkin.aws)

# zipkin-aws
Shared libraries that provide Zipkin integration with AWS Kinesis, SQS, 
and X-Ray. Requires JRE 8 or later.

# Usage
These components provide Zipkin 
[Reporters](https://github.com/openzipkin/zipkin-reporter-java/blob/master/core/src/main/java/zipkin2/reporter/Reporter.java) and 
[Senders](https://github.com/openzipkin/zipkin-reporter-java/blob/master/core/src/main/java/zipkin2/reporter/Sender.java),
which build off interfaces provided by the [zipkin-reporters-java](https://github.com/openzipkin/zipkin-reporter-java), and
and [Collectors](https://github.com/openzipkin/zipkin/blob/master/zipkin-collector/core/src/main/java/zipkin2/collector/Collector.java),
which are used in a [Zipkin](https://github.com/openzipkin/zipkin) server.

## Reporters and Senders
The component in a traced application that sends timing data (spans)
out of process is called a Reporter.
It is responsible for handling the queueing and encoding of 
outbound spans.

Reporters that are sending Zipkin data to Zipkin typically make use of a 
Sender, which implements the wire protocol to a particular technology
used to send the encoded spans.
Typically Senders are called on interval by an
[async reporter](https://github.com/openzipkin/zipkin-reporter-java#asyncreporter).

NOTE: Applications can be written in any language. While we currently
only have Reporters and Senders in Java, senders in other languages 
are welcome.

Reporter | Description
--- | ---
[X-Ray UDP](./reporter-xray-udp) | Reports spans to [X-Ray](https://aws.amazon.com/xray/), AWS's alternative to Zipkin.

Sender | Description
--- | ---
[SQS](./collector-sqs) | Sends tracing data to Zipkin using [SQS](https://aws.amazon.com/sqs/), a message queue service.
[Kinesis](./collector-kinesis) | Sends tracing data to Zipkin using [Kinesis](https://aws.amazon.com/kinesis/), an alternative similar to Kafka.

## Collectors
The component in a zipkin server that receives trace data is called a
collector. This decodes spans reported by applications and persists them
to a configured storage component.

Collector | Description
--- | ---
[SQS](./collector-kinesis) | An alternative to Kafka.
[Kinesis](./collector-kinesis) | An alternative to Kafka.

## Server integration
In order to integrate with zipkin-server, you need to use properties
launcher to load your collector (or sender) alongside the zipkin-server
process.

To integrate a module with a Zipkin server, you need to:
* add a module jar to the `loader.path`
* enable the profile associated with that module
* launch Zipkin with `PropertiesLauncher`

Each module will also have different minimum variables that need to be set.

Ex.
```
$ java -Dloader.path=sqs.jar -Dspring.profiles.active=sqs -cp zipkin.jar org.springframework.boot.loader.PropertiesLauncher
```

## Example integrating the SQS Collector

If you cannot use our [Docker image](https://github.com/openzipkin/docker-zipkin-aws), you can still integrate
yourself by downloading a couple jars. Here's an example of integrating the SQS Collector.

### Step 1: Download zipkin-server jar
Download the [latest released server](https://search.maven.org/remote_content?g=io.zipkin&a=zipkin-server&v=LATEST&c=exec) as zipkin.jar:

```bash
cd /tmp
curl -sSL https://zipkin.io/quickstart.sh | bash -s
```

### Step 2: Download the latest sqs-module jar
Download the [latest released SQS module](https://search.maven.org/remote_content?g=io.zipkin.aws&a=zipkin-module-collector-sqs&v=LATEST&c=module) as sqs.jar:

```
cd /tmp
curl -sSL https://zipkin.io/quickstart.sh | bash -s io.zipkin.aws:zipkin-module-collector-sqs:LATEST:module sqs.jar
```

### Step 3: Run the server with the "sqs" profile active
When you enable the "sqs" profile, you can configure sqs with
short environment variables similar to other [Zipkin integrations](https://github.com/openzipkin/zipkin/blob/master/zipkin-server/README.md#elasticsearch-storage).

``` bash
cd /tmp
SQS_QUEUE_URL=<from aws sqs list-queues> \
    java \
    -Dloader.path='sqs.jar,sqs.jar!/lib' \
    -Dspring.profiles.active=sqs \
    -cp zipkin.jar \
    org.springframework.boot.loader.PropertiesLauncher
```
** NOTE: Make sure the parameters are defined in the same line or use environment variables **

## Artifacts
All artifacts publish to the group ID "io.zipkin.aws". We use a common
release version for all components.

### Library Releases
Releases are uploaded to [Bintray](https://bintray.com/openzipkin/maven/zipkin) and synchronized to [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.zipkin.aws%22)
### Library Snapshots
Snapshots are uploaded to [JFrog](https://oss.jfrog.org/artifactory/oss-snapshot-local) after commits to master.
