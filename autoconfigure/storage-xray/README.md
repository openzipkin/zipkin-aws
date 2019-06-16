# autoconfigure-storage-xray

## Overview

This is a Spring Boot [AutoConfiguration](http://docs.spring.io/spring-boot/docs/current/reference/html/using-boot-auto-configuration.html)
module that can be added to a [Zipkin Server](https://github.com/openzipkin/zipkin/tree/master/zipkin-server) 
deployment to send Spans to Amazon XRay.

This currently only supports sending to an XRay UDP daemon, not reading back spans from the service.
Internally this module wraps the [XRayUDPStorage](https://github.com/openzipkin/zipkin-aws/tree/master/storage-xray-udp)
and exposes configuration options through environment variables.

## Experimental
* Note: This is currently experimental! *
* Note: This requires reporters send 128-bit trace IDs, with the first 32bits as epoch seconds *
* Check https://github.com/openzipkin/b3-propagation/issues/6 for tracers that support epoch128 trace IDs

## Quick start

JRE 8 is required to run Zipkin server.

Fetch the latest released
[executable jar for Zipkin server](https://search.maven.org/remote_content?g=io.zipkin&a=zipkin-server&v=LATEST&c=exec)
and
[autoconfigure module jar for the xray storage](https://search.maven.org/remote_content?g=io.zipkin.aws&a=zipkin-autoconfigure-storage-xray&v=LATEST&c=module).
Run Zipkin server with the XRAY storage enabled.

For example:

```bash
$ curl -sSL https://zipkin.io/quickstart.sh | bash -s
$ curl -sSL https://zipkin.io/quickstart.sh | bash -s io.zipkin.aws:zipkin-autoconfigure-storage-xray:LATEST:module xray.jar
$ STORAGE_TYPE=xray \
    java \
    -Dloader.path='xray.jar,xray.jar!/lib' \
    -Dspring.profiles.active=xray \
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
[reference](https://github.com/openzipkin/zipkin-aws/tree/master/autoconfigure/storage-xray/src/main/resources/zipkin-server-xray.yml)
for users that prefer a file based approach.

##### Environment Variables

- `AWS_XRAY_DAEMON_ADDRESS` The UDP endpoint to send spans to. _Defaults to localhost:2000_

### Testing

Once your storage is enabled, verify it is running:
```bash
$ curl -s localhost:9411/health|jq .zipkin.details.XRayUDPStorage
{
  "status": "UP"
}
```
