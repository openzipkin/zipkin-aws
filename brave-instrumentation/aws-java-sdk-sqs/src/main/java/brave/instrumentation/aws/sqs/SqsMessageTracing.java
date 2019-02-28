package brave.instrumentation.aws.sqs;

import brave.Tracing;
import com.amazonaws.handlers.RequestHandler2;

public class SqsMessageTracing {
    public static SqsMessageTracing create(Tracing tracing) {
        return new SqsMessageTracing(tracing);
    }

    final RequestHandler2 requestHandler;

    private SqsMessageTracing(Tracing tracing) {
        this.requestHandler = new SendMessageTracingRequestHandler(tracing);
    }

    public RequestHandler2 getRequestHandler() {
        return requestHandler;
    }
}
