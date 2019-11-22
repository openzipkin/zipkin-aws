# AWS X-Ray Reporter for Zipkin

Used in an application that is being traced, this component converts Zipkin 
trace data (spans) into [AWS X-Ray](https://aws.amazon.com/xray/)'s
proprietary format and sends them to the 
[X-Ray daemon](https://docs.aws.amazon.com/xray/latest/devguide/xray-daemon.html) 
using UDP.

Note that, unlike `AsyncReporter`, this reporter attempts to encode and send
the span immediately on the calling thread.
As UDP is used, there is no latency in waiting for the daemon to accept and 
respond to the data.
 
## Configuration

To use the X-Ray Reporter, you'll need to:

### 1. Include the dependency in your application

The coordinates of the dependency to include are:

`io.zipkin.aws:zipkin-reporter-xray-udp:<VERSION>`

Note that this also brings in the required `zipkin-storage-xray-udp` dependency.

### 2. Create a Tracing instance that uses the Reporter

For example, if you're tracing in a Spring application, you can configure the
Tracing bean as such:

```$java
    @Bean
    fun tracing(@Value("\${spring.application.name:spring-tracing}") serviceName: String, 
                spanReporter: Reporter<Span>): Tracing {
        return Tracing.newBuilder()
            .localServiceName(serviceName)
            .spanReporter(XRayUDPReporter.create())
            .traceId128Bit(true) // X-Ray requires 128-bit trace IDs
            .sampler(Sampler.ALWAYS_SAMPLE) // Configure as desired
            .build()
    }
```

### 3. Ensure the X-Ray daemon is running alongside your application

The Reporter doesn't send tracing data directly to the X-Ray product, 
but to an X-Ray daemon accessible locally via UDP.
You'll need to ensure your application has access to a running X-Ray daemon
for tracing data to reach X-Ray. AWS provides documentation for
[coniguring the X-Ray daemon in various environments](https://docs.aws.amazon.com/xray/latest/devguide/xray-daemon.html). 
Don't forget to give the infrastructure hosting your X-Ray daemon
[permission to upload traces to X-Ray in IAM](https://docs.aws.amazon.com/xray/latest/devguide/xray-permissions.html).

## Configuring the X-Ray Daemon address

The default address used to send to the daemon is `localhost:2000`.

If you need to override the default address, either:
1. set the environment variable `AWS_XRAY_DAEMON_ADDRESS`; or
2. pass the address as a string to `XRayUDPReporter.create()`.



## AWS Propagation

If you are using components of AWS that natively send tracing to X-Ray
(as opposed to only tracing with Zipkin throughout the stack), 
you will want to consider switching to 
[AWS Trace Propagation](https://github.com/openzipkin/zipkin-aws/tree/master/brave-propagation-aws). 