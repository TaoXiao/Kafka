package cn.gridx.kafka.apis.scala.producer

import java.util
import java.util.Properties

import cn.gridx.kafka.apis.scala.serialization.IntegerSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.StringSerializer

/**
 * Created by tao on 1/5/16.
 */

    object RetrievePartitions extends App {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG       , "ecs1:9092,ecs2:9092,ecs3:9092")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG    , classOf[IntegerSerializer].getName)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG  , classOf[StringSerializer].getName)

        val producer = new KafkaProducer[Nothing, Nothing](props)

        val partitions: util.List[PartitionInfo] = producer.partitionsFor("topic-B")
        for (i <- Range(0, partitions.size)) {
            val parInfo = partitions.get(i)
            println(s"Topic: ${parInfo.topic},  Partition-Id: ${parInfo.partition},  Leader: ${parInfo.leader}")
        }
    }


