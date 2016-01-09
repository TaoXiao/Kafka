package cn.gridx.kafka.apis.scala.producer

import java.util.Properties
import java.util.concurrent.Future

import cn.gridx.kafka.apis.scala.serialization.IntegerSerializer
import org.apache.kafka.clients.producer.{RecordMetadata, ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

/**
* Created by tao on 6/24/15.
*/

object ProduceKeyedMsg extends App {
    val Topic = args(0)
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG       , "ecs1:9092,ecs2:9092,ecs3:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG    , classOf[IntegerSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG  , classOf[StringSerializer].getName)

    val producer = new KafkaProducer[Int, String](props)
    val numPartitions = producer.partitionsFor(Topic).size()    // 计算出“topic-B”的partitions的数量

    println(s""" Topic \"$Topic\" 有$numPartitions 个partitions """)

    for (i <- 0 to 16) {
        val partitionId = i%numPartitions   // 用key取模的方式确定每条消息的partitionId
        val meta: Future[RecordMetadata] = producer.send(
                new ProducerRecord(Topic, partitionId, i, s""""msg-$i""""))
        println(s"Key: $i,  Partition-Id: ${meta.get.partition}")
    }

    producer.close()
}
