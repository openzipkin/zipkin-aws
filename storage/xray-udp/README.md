# storage-xray-udp

Encoding and wire protocol for sending Zipkin spans to AWS X-Ray.

See [reporter-xray-udp](../reporter-xray-udp) for instructions on setting up
X-Ray reporting for an application.

## Overview
This storage module performs three activites:
 1. Receives incoming zipkin spans
 2. Encodes received zipkin spans to X-Ray compatible segments
 3. Forwards the encoded X-Ray segments to a X-Ray daemon endpoint

The encoder is implemented by `zipkin2.storage.xray_udp.UDPMessageEncoder` class.

### Field Mappings

| Name | Source | Destination | Mandatory | Transformation | Reference |
|------|--------|-------------|-----------|----------------|-----------|
| Origin | `span.tags['aws.origin']` | `segment.origin` | No | Simple value mapped | [**Segment fields**](https://docs.aws.amazon.com/xray/latest/devguide/xray-api-segmentdocuments.html#api-segmentdocuments-fields) > **Optional Segment Fields** > `origin` |

### HttpTracing

In order to enable tracing of the URL and status code.  The `HttpTracing` needs to be built with a builder rather than `.create` as the default implementation does not provide that information to the trace as it may contain sensitive information.  The following code shows how to enable it as per https://github.com/openzipkin/zipkin-aws/issues/58#issuecomment-1036995157

        return HttpTracing.newBuilder(tracing)
            .serverRequestParser(
                (req, context, span) -> {
                    HttpRequestParser.DEFAULT.parse(req, context, span);
                    HttpTags.URL.tag(req, context, span);
                }
            )
            .serverResponseParser(
                ((response, context, span) -> {
                    HttpResponseParser.DEFAULT.parse(response, context, span);
                    HttpTags.STATUS_CODE.tag(response, span);
                })
            )
            .clientRequestParser(
                (req, context, span) -> {
                    HttpRequestParser.DEFAULT.parse(req, context, span);
                    HttpTags.URL.tag(req, context, span);
                }
            )
            .clientResponseParser(
                ((response, context, span) -> {
                    HttpResponseParser.DEFAULT.parse(response, context, span);
                    HttpTags.STATUS_CODE.tag(response, span);
                })
            )
            .build();
