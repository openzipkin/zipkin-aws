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

## Usage

Download the module from [TODO] link and extract it to a directory relative to the
Zipkin Server jar.

### Configuration

Configuration can be applied either through environment variables or an external Zipkin
configuration file.  The module includes default configuration that can be used as a 
[reference](https://github.com/openzipkin/zipkin-aws/tree/master/autoconfigure/storage-xray/src/main/resources/zipkin-server-xray.yml)
for users that prefer a file based approach.

##### Environment Variables

- `AWS_XRAY_DAEMON_ADDRESS` The UDP endpoint to send spans to. _Defaults to localhost:2000_

### Running

```bash
STORAGE_TYPE=xray
java -Dloader.path=xray -Dspring.profiles.active=xray -cp zipkin.jar org.springframework.boot.loader.PropertiesLauncher
```

### Testing

Once your storage is enabled, verify it is running:
```bash
$ curl -s localhost:9411/health|jq .zipkin.XrayStorage
{
  "status": "UP"
}
```
