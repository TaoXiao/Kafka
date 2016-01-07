package cn.gridx.kafka.apis.scala.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

import scala.util.Random

/**
 * Created by tao on 1/4/16.
 */

/**
 * 只产生没有key的消息，且topic固定
 *
 * @param brokers A list of host/port pairs to use for establishing
 *     the initial connection to the Kafka cluster. The client will
 *     make use of all servers irrespective of which servers are
 *     specified here for bootstrapping—this list only impacts the
 *     initial hosts used to discover the full set of servers. This
 *     list should be in the form host1:port1,host2:port2,....
 *     Since these servers are just used for the initial connection to
 *     discover the full cluster membership (which may change dynamically),
 *     this list need not contain the full set of servers (you may want
 *     more than one, though, in case a server is down).
 */

class ProduceUnkeyedMsg() {
    /**
     * `bootstrap.servers` 形为 "ecs1:9092,ecs2:9092,ecs3:9092,ecs4:9092"
     * key.serializer和value.serializer是必须要指定的
     * 即使消息不含 key，配置中也必须指定 key.serializer
     */
    val props = new Properties()
    props.put("bootstrap.servers", "ecs1:9092,ecs2:9092,ecs3:9092,ecs4:9092")
    props.put("key.serializer"   , classOf[org.apache.kafka.common.serialization.StringSerializer])
    props.put("value.serializer" , classOf[org.apache.kafka.common.serialization.StringSerializer])

    /**
     * KafkaProducer必须带有类型参数[K, V]，分别是key和value的类型
     * 如果消息中不含key，则K可指定为Nothing
     */
    val producer = new KafkaProducer[Nothing, String](props)

    // 每隔0.5秒产生一个随机字符
    def startProduceChars(count: Int): Unit = {
        for (i <- Range(0, count)) {
            /**
             * `ProducerRecord`有多种不同的构造函数
             * 这里选择的是不含 key 和 partition number 的构造函数
             * partition number 将按照round-robin的方式确定
             *
             * `send`方法是异步的，它将消息放入发送队列后就立即返回
             */
            producer.send(new ProducerRecord("topic-B", Random.nextPrintableChar().toString))
            Thread.sleep(500)
        }

        // 关闭资源
        producer.close()
    }
}


object ProduceUnkeyedMsg extends App {
    new ProduceUnkeyedMsg().startProduceChars(10);
}

