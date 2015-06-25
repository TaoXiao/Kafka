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
    def TOPIC = "my-2nd-topic"

    def main(args: Array[String]): Unit = {
        println("开始产生消息！")

        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

        val producer = new KafkaProducer[String, String](props)

        /*
            一个典型的输出为（TOPIC 有2个partition，每个partition有3个replica）
            i=0,   offset=242,  partition=1
            i=1,   offset=186,  partition=0
            i=2,   offset=187,  partition=0
            i=3,   offset=243,  partition=1
            i=4,   offset=244,  partition=1
            i=5,   offset=188,  partition=0
            i=6,   offset=189,  partition=0
            i=7,   offset=245,  partition=1
            i=8,   offset=246,  partition=1
            i=9,   offset=247,  partition=1
            i=10,  offset=190,  partition=0
            i=11,  offset=248,  partition=1
            i=12,  offset=191,  partition=0
            i=13,  offset=249,  partition=1
            i=14,  offset=192,  partition=0
            i=15,  offset=250,  partition=1
            i=16,  offset=251,  partition=1
            i=17,  offset=193,  partition=0
            i=18,  offset=252,  partition=1
            i=19,  offset=194,  partition=0
            i=20,  offset=253,  partition=1
         */
        for (i <- 0 to 20) {
            val ret: Future[RecordMetadata] = producer.send(new ProducerRecord(TOPIC, "key-" + i, "msg-" + i))
            val metadata = ret.get  // 打印出 metadata
            print("i=" + i + ",  offset=" + metadata.offset() + ",  partition=" + metadata.partition())
        }

        producer.close

    }
}
