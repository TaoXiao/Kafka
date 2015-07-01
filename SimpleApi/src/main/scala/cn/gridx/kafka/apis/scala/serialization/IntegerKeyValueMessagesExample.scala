package cn.gridx.kafka.apis.scala.serialization

import java.util.Properties

import kafka.consumer.{KafkaStream, Consumer, ConsumerConfig}
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}

/**
 * Created by tao on 7/1/15.
 */
object IntegerKeyValueMessagesExample {
    def Topic       = "my-3rd-topic"
    def Broker_List = "ecs2:9092,ecs3:9092,ecs4:9092"
    def ZK_CONN     = "ecs1:2181,ecs2:2181,ecs3:2181"
    def Group_ID    = "group-SimpleProducerExample"

    def main(args: Array[String]): Unit = {
        startProducer()
        Thread.sleep(2000)
        startConsumer()
    }

    def startProducer(): Unit = {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Broker_List)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer])

        val producer = new KafkaProducer[Int, Int](props)

        for (i <- 0 to 20 if i%3 == 0)
            producer.send(new ProducerRecord(Topic, i, i*100))

        producer.close
    }

    def startConsumer(): Unit = {
        val props = new Properties()
        props.put("zookeeper.connect", ZK_CONN)
        props.put("group.id", Group_ID)
        props.put("zookeeper.session.timeout.ms", "400")
        props.put("zookeeper.sync.time.ms", "200")
        props.put("auto.commit.interval.ms", "1000")
        val config = new ConsumerConfig(props)

        val topicCountMap = Map(Topic -> 1)
        val connector = Consumer.create(config)
        val topicStreamsMap: collection.Map[String, List[KafkaStream[Int, Int]]] = connector.createMessageStreams(topicCountMap,
                new IntegerDecoder(), new IntegerDecoder())

        val stream = topicStreamsMap.get(Topic).get.head
        val it = stream.iterator

        while (it.hasNext) {
            val data = it.next
            println(s"key ->[${data.key}]  |  value -> [${data.message}}]")
        }
    }
}
