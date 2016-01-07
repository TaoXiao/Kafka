package cn.gridx.kafka.apis.scala.producer

import java.util.Properties

import kafka.consumer._
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer



/**
 * Created by tao on 6/30/15.
 *
 * 产生若干个不带key的`ProducerRecord`(key就是null/Nothing),
 * 将这些record发送至一个指定的Topic（不指定partition number）,
 *
 */
object SimpleProducerExample {
    // 这个Topic在创建时设置了5个partitions
    def Topic       = "my-3rd-topic"
    def Broker_List = "ecs2:9092,ecs3:9092,ecs4:9092"
    def ZK_CONN     = "ecs1:2181,ecs2:2181,ecs3:2181"
    def Group_ID    = "group-SimpleProducerExample"

    def main(args: Array[String]): Unit = {
        startProducer

        println("Production 完成")
        Thread.sleep(3000)
        println("Consumption 开始")

        startConsumer
    }

    /**
     * producer 产生messages
     */
    def startProducer() {
        val props = new Properties()

        // bootstrap.servers=ecs2:9092,ecs3:9092,ecs4:9092
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Broker_List)
        // 必须要为`key`和`value`设置`serializer`，即使我们不使用key也必须要设置
        // key.serializer=classOf[StringSerializer] | value.serializer=classOf[StringSerializer]
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

        /*  由于我们不产生key，只产生String类型的value
         *  因此prducer的类型是 KafkaProducer[Nothing, String]
         */
        val producer = new KafkaProducer[Nothing, String](props)

        for (i <- 0 to 10) {
           /*   A key/value pair to be sent to Kafka. This consists of a topic name to which the record is being sent, an optional
                partition number, and an optional key and value.

                ·   If a valid partition number is specified that partition will be used when sending the record.
                ·   If no partition is specified but a key is present a partition will be chosen using a hash of the key
                    （除了hash之外，是否有可定义的方式？）.
                ·   If neither key nor partition is present a partition will be assigned in a round-robin fashion.
            */
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

        // Topic Name -> # of streams for the specified topic
        val topicCountMap = Map(Topic -> 1)

        val connector: ConsumerConnector = Consumer.create(config)
        val topicStreamsMap: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]]
                = connector.createMessageStreams(topicCountMap)

        println(s"Topic ${Topic} 对应有 ${topicStreamsMap.get(Topic).size} 个KafkaStream")

        // 我们为该Topic只设置了一个KafkaStream，所以这里直接用head来取第一个元素（唯一的stream）即可
        val stream: KafkaStream[Array[Byte], Array[Byte]] = topicStreamsMap.get(Topic).get.head
        val it: ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator()

        // 依次取出stream中的每一个记录（带metadata）
        while (it.hasNext) {
            val data: MessageAndMetadata[Array[Byte], Array[Byte]] = it.next
            println(s"value -> [${new String(data.message)}] | partition -> [${data.partition}] | " +
                s"offset -> [${data.offset}] | topic -> [${data.topic}]")
        }
    }
}
