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

## Environment Variables
Use the following environment variables to customize the storage settings.

| Name | Mapped Java Type | Default | Valid Values | Purpose |
|------|------------------|---------|--------------|---------|
| `AWS_XRAY_ORIGIN` | `String` | `null` | Supported origin values as per AWS X-Ray segment `origin` specification. | Value to populate the `origin` field of the X-Ray segment document. |