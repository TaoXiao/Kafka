package cn.gridx.kafka.apis.scala.producer

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer

/**
 * Created by tao on 1/5/16.
 */
object Acknowledgements  {
    def main(args: Array[String]): Unit = {
        TestAcks(args(0))
    }

    def TestAcks(ack: String): Unit = {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG       , "ecs1:9092,ecs2:9092")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG    , classOf[StringSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG  , classOf[StringSerializer])
        props.put(ProducerConfig.ACKS_CONFIG                    , ack)
        val producer = new KafkaProducer[String, String](props)

        val futures = ArrayBuffer[java.util.concurrent.Future[RecordMetadata]]()

        for (i <- Range(0, 10)) {
            println(s"调用时间: [${DateTime.now().toString("HH:mm:ss")}],  产生第 $i 条消息")
            val record = new ProducerRecord("topic-B", i.toString, (i*100).toString)

            val future: Future[RecordMetadata] = producer.send(record)
            futures.append(future)
        }

        /**
         * 调用`java.util.concurrent.Future#get`会等待这个future的任务完成，然后取回结果
         */
        for (future <- futures) {
            val meta = future.get()
            println(s"执行时间: [${DateTime.now().toString("HH:mm:ss")}],  " +
                    s"Topic: ${meta.topic},  Partition: ${meta.partition},  Offset: ${meta.offset}")
        }

        producer.close()
    }

}
