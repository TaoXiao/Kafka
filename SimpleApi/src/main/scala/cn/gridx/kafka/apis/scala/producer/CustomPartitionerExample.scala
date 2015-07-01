package cn.gridx.kafka.apis.scala.producer

import java.util.Properties
import kafka.consumer._
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

/**
 * Producer 可以为每一个record 的partition number
 *
 * 在0.8.2之前，可以通过 `partitioner.class` 来设置
 *
 * 在0.8.2，这个配置不起作用了，而是改为在构造`ProducerRecord`
 * 时直接填入 partition number
 *
 * Created by tao on 6/29/15.
 */
object CustomPartitionerExample {
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


    def startProducer(): Unit = {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Broker_List)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        // 在0.8.2中`partitioner.class`不再起作用
        //props.put("partitioner.class", "cn.gridx.kafka.apis.scala.producer.CustomPartitionerExample.MyPartitioner")

        val producer = new KafkaProducer[String, String](props)

        for (i <- 0 to 10) {
            // 这里的`i%2`实际上就是为每一个record指定其partition number，必须是一个合法的数字
            val record = new ProducerRecord(Topic, i%2, i.toString, "val_"+i.toString)
            producer.send(record)
        }

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

        val connector: ConsumerConnector = Consumer.create(config)
        val topicStreamsMap: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]]
        = connector.createMessageStreams(topicCountMap)

        val stream: KafkaStream[Array[Byte], Array[Byte]] = topicStreamsMap.get(Topic).get.head
        val it: ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator()

        while (it.hasNext) {
            val data: MessageAndMetadata[Array[Byte], Array[Byte]] = it.next
            // 现在要打印出每一条Message的key
            println(s"key -> [${new String(data.key)}}] | value -> [${new String(data.message)}] | " +
                    s"partition -> [${data.partition}] | offset -> [${data.offset}] | topic -> [${data.topic}]")
        }
    }
}
