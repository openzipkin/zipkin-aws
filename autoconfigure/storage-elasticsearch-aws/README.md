# autoconfigure-storage-elasticsearch-aws

## Overview

This is a Spring Boot [AutoConfiguration](http://docs.spring.io/spring-boot/docs/current/reference/html/using-boot-auto-configuration.html)
module that can be added to a [Zipkin Server](https://github.com/openzipkin/zipkin/tree/master/zipkin-server) 
deployment to store spans in [Amazon Elasticsearch Service](https://aws.amazon.com/elasticsearch-service/).

## Quick start

JRE 8 is required to run Zipkin server.

Before you start, make sure your cli credentials are setup as zipkin
will read them:
```bash
$ aws es describe-elasticsearch-domain --domain-name mydomain|jq .DomainStatus.Endpoint
"search-mydomain-2rlih66ibw43ftlk4342ceeewu.ap-southeast-1.es.amazonaws.com"
```

Fetch the latest released
[executable jar for Zipkin server](https://search.maven.org/remote_content?g=io.zipkin.java&a=zipkin-server&v=LATEST&c=exec)
and
[autoconfigure module jar for the Elasticsearch AWS storage](https://search.maven.org/remote_content?g=io.zipkin.aws&a=zipkin-autoconfigure-storage-elasticsearch-aws&v=LATEST&c=module).
Run Zipkin with elasticsearch storage enabled and your AWS URL

For example:

```bash
$ curl -sSL https://zipkin.io/quickstart.sh | bash -s
$ curl -sSL https://zipkin.io/quickstart.sh | bash -s io.zipkin.aws:zipkin-autoconfigure-storage-elasticsearch-aws:LATEST:module elasticsearch-aws.jar
$ STORAGE_TYPE=elasticsearch ES_HOSTS=https://search-mydomain-2rlih66ibw43ftlk4342ceeewu.ap-southeast-1.es.amazonaws.com \
    java \
    -Dloader.path='elasticsearch-aws.jar,elasticsearch-aws.jar!/lib' \
    -Dspring.profiles.active=elasticsearch-aws \
    -cp zipkin.jar \
    org.springframework.boot.loader.PropertiesLauncher
```

Alternatively, you can have zipkin implicitly lookup your domain's URL:
```bash
$ STORAGE_TYPE=elasticsearch ES_AWS_DOMAIN=mydomain ES_AWS_REGION=ap-southeast-1 \
    java \
    -Dloader.path='elasticsearch-aws.jar,elasticsearch-aws.jar!/lib' \
    -Dspring.profiles.active=elasticsearch-aws \
    -cp zipkin.jar \
    org.springframework.boot.loader.PropertiesLauncher
```


After executing these steps, applications can send spans
http://localhost:9411/api/v2/spans (or the legacy endpoint http://localhost:9411/api/v1/spans)

The Zipkin server can be further configured as described in the
[Zipkin server documentation](https://github.com/openzipkin/zipkin/blob/master/zipkin-server/README.md).

### Configuration

Configuration can be applied either through environment variables or an
external Zipkin configuration file.  The module includes default
configuration that can be used as a [reference](https://github.com/openzipkin/zipkin-aws/tree/master/autoconfigure/storage-elasticsearch-aws/src/main/resources/zipkin-server-elasticsearch-aws.yml) for users that prefer a
file based approach.

##### Environment Variables

- `ES_HOSTS` The http URL is an AWS-hosted elasticsearch installation
             (e.g. https://search-domain-xyzzy.us-west-2.es.amazonaws.com)
             Zipkin will attempt the default AWS credential provider
             (env variables, system properties, config files, or ec2
             profiles) to sign outbound requests to the cluster.

- `ES_AWS_DOMAIN` The name of the AWS-hosted elasticsearch domain to use.
                  Supercedes any `ES_HOSTS`. Triggers the same request
                  signing behavior as with `ES_HOSTS`, but requires the
                  additional IAM permission to describe the given domain.
- `ES_AWS_REGION` An optional override to the default region lookup to
                  search for the domain given in `ES_AWS_DOMAIN`.
                  Ignored if only `ES_HOSTS` is present.

### Testing

Once your storage is enabled, verify it is running:
```bash
$ curl -s localhost:9411/health|jq .zipkin.details.ElasticsearchStorage
{
  "status": "UP"
}
```
