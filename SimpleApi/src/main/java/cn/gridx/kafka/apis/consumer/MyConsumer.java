package cn.gridx.kafka.apis.consumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by tao on 6/21/15.
 *
 * 这是一个最简单的consumer
 *
 *  用途： 启动一个consumer线程，从一个名为`xt-group-1`的topic中读取消息，并将其显示出来
 *
 *  亲测可用
 *
 *
 *  参考： <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-examples</artifactId>
            <version>0.8.2.0</version>
          </dependency>
 */
public class MyConsumer extends Thread {
    private static final String ZK_CONN = "ecs1:2181,ecs2:2181,ecs3:2181";
    private static final String GROUP_ID = "xt-group-1";

    private ConsumerConnector consumerConn;
    private String topic;

    public static void main(String[] args) {
        MyConsumer myConsumerThread = new MyConsumer("my-2nd-topic");
        myConsumerThread.start();
    }


    public MyConsumer(String topic) {
        this.topic = topic;
        consumerConn = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
    }

    private static ConsumerConfig createConsumerConfig()
    {
        Properties props = new Properties();
        props.put("zookeeper.connect", ZK_CONN);
        props.put("group.id", GROUP_ID);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);    // 这个topic只有1个partition ?

        Map<String, List<KafkaStream<byte[], byte[]>>> msgStreams = consumerConn.createMessageStreams(topicCountMap);

        KafkaStream<byte[], byte[]> stream  = msgStreams.get(topic).get(0);

        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while (it.hasNext())
            System.out.println("<" + new String(it.next().message()) + ">");
    }

}
