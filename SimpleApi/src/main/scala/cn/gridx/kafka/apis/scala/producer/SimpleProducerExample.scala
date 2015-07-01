package cn.gridx.kafka.apis.scala.producer

import java.util.Properties

import kafka.consumer._
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.{RecordMetadata, ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer



/**
 * Created by tao on 6/30/15.
 */
object SimpleProducerExample {
    // 这个Topic在创建时设置了5个partitions
    def Topic       = "my-3rd-topic"
    def Broker_List = "ecs2:9092,ecs3:9092,ecs4:9092"
    def ZK_CONN     = "ecs1:2181,ecs2:2181,ecs3:2181"
    def Group_ID    = "group-SimpleProducerExample"

    def main(args: Array[String]): Unit = {
        startProducer
        Thread.sleep(3000)
        println("发送完成")
        startConsumer
    }

    /**
     * producer 产生messages
     */
    def startProducer() {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Broker_List)
        // 必须要为`key`和`value`设置`serializer`，即使我们不使用key也必须要设置
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

        // 由于我们不产生key，只产生String类型的value
        // 因此prducer的类型是 KafkaProducer[Nothing, String]
        val producer = new KafkaProducer[Nothing, String](props)

        for (i <- 0 to 10) {
            // 只指定topic
            val record: ProducerRecord[Nothing, String] = new ProducerRecord(Topic, s"val-${i}")
            producer.send(record)
        }

        producer.close  // 完了必须关闭producer
    }


    /**
     * consumer 消费 messages
     */
    def startConsumer(): Unit = {
        val props = new Properties()
        props.put("zookeeper.connect", ZK_CONN)
        props.put("group.id", Group_ID)
        props.put("zookeeper.session.timeout.ms", "400")
        props.put("zookeeper.sync.time.ms", "200")
        props.put("auto.commit.interval.ms", "1000")
        val config = new ConsumerConfig(props)

        val topicCountMap = Map(Topic -> 1)

        val connector: ConsumerConnector = Consumer.create(config)
        val topicStreamsMap: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]]
                = connector.createMessageStreams(topicCountMap)

        val stream: KafkaStream[Array[Byte], Array[Byte]] = topicStreamsMap.get(Topic).get.head
        val it: ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator()

        while (it.hasNext) {
            val data: MessageAndMetadata[Array[Byte], Array[Byte]] = it.next
            println(s"value -> [${new String(data.message)}] | partition -> [${data.partition}] | " +
                s"offset -> [${data.offset}] | topic -> [${data.topic}]")
        }
    }
}
