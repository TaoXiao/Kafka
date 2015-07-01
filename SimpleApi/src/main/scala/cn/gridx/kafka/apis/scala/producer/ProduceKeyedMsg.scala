package cn.gridx.kafka.apis.scala.producer

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{RecordMetadata, ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

/**
 * Created by tao on 6/24/15.
 */
object ProduceKeyedMsg {
    def BROKER_LIST = "ecs1:9092,ecs2:9092"
    def TOPIC = "my-3rd-topic"


    def main(args: Array[String]): Unit = {
        println("开始产生消息！")

        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

        val producer = new KafkaProducer[Int, String](props)

        var totalCount = 0

        for (i <- 0 to 16) {
            val ret: Future[RecordMetadata] = producer.send(new ProducerRecord(TOPIC, i, s"value-${i.toString}"))
            val metadata = ret.get  // 打印出 metadata
            totalCount += i
            println("i=" + i + " | offset=" + metadata.offset() + "  |  partition=" + metadata.partition())
        }

        println(s"$totalCount messages where sent")

        producer.close
    }
}
