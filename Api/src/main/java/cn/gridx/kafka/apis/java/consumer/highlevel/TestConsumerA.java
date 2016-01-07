package cn.gridx.kafka.apis.java.consumer.highlevel;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
* Created by tao on 1/6/16.
*/
public class TestConsumerA {
  public static void main(String[] args) throws InterruptedException {
    Properties props = new Properties();
    props.put("group.id", "gid-1");
    props.put("zookeeper.connect", "ecs2:2181,ecs3:2181,ecs4:2181/kafka");
    props.put("auto.offset.reset", "largest");
    props.put("auto.commit.interval.ms", "1000");
    props.put("partition.assignment.strategy", "roundrobin");
    ConsumerConfig config = new ConsumerConfig(props);

    String topic = "topic-C";

    /** 创建 `kafka.javaapi.consumer.ConsumerConnector` */
    ConsumerConnector consumerConn =
        kafka.consumer.Consumer.createJavaConsumerConnector(config);

    /** 设置 map of (topic -> #streams ) */
    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(topic, 3);

    /** 创建 map of (topic -> list of streams) */
    //Map<String, List<KafkaStream<byte[], byte[]>>> topicStreamsMap =
    //    consumerConn.createMessageStreams(topicCountMap);

    /** 在这里可以为key和value直接提供各自的 decoder,
     * 这样consumer thread 收到的解码后的数据
     * */
    Map<String, List<KafkaStream<Integer, String>>> topicStreamsMap =
      consumerConn.createMessageStreams(topicCountMap, new IntDecoder(), new StringDecoder());

    /** 取出 `topic-B` 对应的 streams */
    List<KafkaStream<Integer, String>> streams = topicStreamsMap.get(topic);

    /** 创建一个容量为3的线程池 */
    ExecutorService executor = Executors.newFixedThreadPool(3);

    /** 创建3个consumer threads */
    for (int i = 0; i < streams.size(); i++)
      executor.execute(new ConsumerA("消费者" + (i + 1), streams.get(i)));

    /** 给3个consumer threads 60秒的时间读取消息，然后令它们停止读取消息
     *  要先断开ConsumerConnector，然后再销毁consumer client threads
     *
     *  如果不调用`consumerConn.shutdown()`，那么这3个消费者线程永远不会结束，
     *  因为只要ConsumerConnector还在的话，consumer会一直等待新消息，不会自己退出
     *  如果ConsumerConnector关闭了，那么consumer中的`hasNext`就会返回false
     *  */
    Thread.sleep(60*1000);
    consumerConn.shutdown();

    /** 给3个consumer threads 5秒的时间，让它们将最后读取的消息的offset保存进ZK
     * */
    Thread.sleep(5*1000);
    executor.shutdown();
  }

  public static class IntDecoder implements Decoder<Integer> {
    @Override
    public Integer fromBytes(byte[] bytes) {
      if (null == bytes || bytes.length != 4)
        throw new IllegalArgumentException("Invalid bytes can not be casted to integer");
      return java.nio.ByteBuffer.wrap(bytes).getInt();
    }
  }

  public static class StringDecoder implements Decoder<String> {
    @Override
    public String fromBytes(byte[] bytes) {
      if (null == bytes )
        throw new IllegalArgumentException("Null bytes");
      return new String(bytes);
    }
  }
}



