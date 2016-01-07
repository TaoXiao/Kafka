package cn.gridx.kafka.apis.scala.producer

import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.joda.time.DateTime


/**
 * Created by tao on 1/6/16.
 *
 * 测试同步的producer
 */
object SyncProducer extends App {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG       , "ecs1:9092,ecs2:9092,ecs3:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG    , classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG  , classOf[StringSerializer])

    val producer  = new KafkaProducer[String, String](props)

    for (i <- Range(0, 10)) {
        val record = new ProducerRecord("topic-B", i.toString, (i*100).toString)
        println(s"调用时间: [${DateTime.now().toString("HH:mm:ss")}], sending 第 $i 条消息 ...  ")

        /** 对`send`调用`get`会造成`send`变为阻塞的  */
        val meta: RecordMetadata = producer.send(record).get()

        println(s"调用时间: [${DateTime.now().toString("HH:mm:ss")}],  " +
                s"Topic: ${meta.topic()}, Partition: ${meta.partition()},  " +
                s"Offset: ${meta.offset()} ")
    }

    producer.close()


}
