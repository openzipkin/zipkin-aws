package zipkin.sparkstreaming.stream.kinesis;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import zipkin.sparkstreaming.StreamFactory;

/**
 * Created by ddcbdevins on 2/19/17.
 */
public class KinesisStreamFactory implements StreamFactory {
    @Override
    public JavaDStream<byte[]> create(JavaStreamingContext javaStreamingContext) {
        return null;
    }
}
