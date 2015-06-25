package cn.gridx.kafka.apis.java.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by tao on 6/24/15.
 */
public class HighLevelConsumer {
    private static final String ZK_CONN = "ecs1:2181,ecs2:2181,ecs3:2181";
    private static final String GROUP_ID = "xt-group-1";
    private static final String TOPIC = "my-2nd-topic";


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("zookeeper.connect", ZK_CONN);
        props.put("group.id", GROUP_ID);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConnector connector = Consumer.create(new ConsumerConfig(props));

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, 3);

        //connector.createMessageStreams(topicCountMap);

    }

}
