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