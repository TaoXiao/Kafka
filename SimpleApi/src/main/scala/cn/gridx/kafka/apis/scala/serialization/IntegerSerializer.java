package cn.gridx.kafka.apis.scala.serialization;

import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

/**
 * Created by tao on 6/30/15.
 */
public class IntegerSerializer implements Serializer<Integer> {
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public byte[] serialize(String topic, Integer data) {
        return java.nio.ByteBuffer.allocate(4).putInt(data).array();
    }

    public void close() {

    }
}
