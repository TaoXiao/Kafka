package cn.gridx.kafka.apis.java.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 本例适用于 kafka 0.8.2
 * Kafka 0.8.2 的API用法与之前的有较大区别
 *
 */
public class SimpleProducer
{
    // 不一定要是cluster中的所有的broker，但至少是2个broker，以防止某个挂掉
    private static final String BROKER_LIST = "ecs1:9092,ecs2:9092";
    private static final String Topic = "my-2nd-topic";

    public static void main( String[] args ) {
        System.out.println( "Hello Kafka!" );

        new SimpleProducer().producerAPI();
    }

    public void producerAPI() {
        System.out.println("start !");

        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer =  new KafkaProducer<String, String>(prop);

        ProducerRecord record = new ProducerRecord(Topic, "xt-key", "xt-value");
        producer.send(record);

        // 一定要close，否则consumer可能无法及时收到发出的消息
        producer.close();

        System.out.println("finished! ");
    }
}
