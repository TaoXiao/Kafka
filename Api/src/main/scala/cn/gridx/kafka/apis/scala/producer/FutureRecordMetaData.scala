package cn.gridx.kafka.apis.scala.producer

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{RecordMetadata, ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer

/**
 * Created by tao on 1/5/16.
 */

object FutureRecordMetaData extends App {
    /**
     * 运行结果
     *
     *  调用时间: [10:28:26],  产生第 0 条消息
        调用时间: [10:28:26],  产生第 1 条消息
        调用时间: [10:28:26],  产生第 2 条消息
        调用时间: [10:28:26],  产生第 3 条消息
        调用时间: [10:28:26],  产生第 4 条消息
        调用时间: [10:28:26],  产生第 5 条消息
        调用时间: [10:28:26],  产生第 6 条消息
        调用时间: [10:28:26],  产生第 7 条消息
        调用时间: [10:28:26],  产生第 8 条消息
        调用时间: [10:28:26],  产生第 9 条消息
        执行时间: [10:28:26],  Topic: topic-B,  Partition: 1,  Offset: 67
        执行时间: [10:28:26],  Topic: topic-B,  Partition: 4,  Offset: 70
        执行时间: [10:28:26],  Topic: topic-B,  Partition: 3,  Offset: 115
        执行时间: [10:28:26],  Topic: topic-B,  Partition: 3,  Offset: 116
        执行时间: [10:28:26],  Topic: topic-B,  Partition: 3,  Offset: 117
        执行时间: [10:28:26],  Topic: topic-B,  Partition: 0,  Offset: 55
        执行时间: [10:28:26],  Topic: topic-B,  Partition: 3,  Offset: 118
        执行时间: [10:28:26],  Topic: topic-B,  Partition: 4,  Offset: 71
        执行时间: [10:28:26],  Topic: topic-B,  Partition: 2,  Offset: 54
        执行时间: [10:28:26],  Topic: topic-B,  Partition: 3,  Offset: 119


     */
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG       , "ecs1:9092,ecs2:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG    , classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG  , classOf[StringSerializer])
    val producer = new KafkaProducer[String, String](props)

    val futures = ArrayBuffer[java.util.concurrent.Future[RecordMetadata]]()

    for (i <- Range(0, 10)) {
        println(s"调用时间: [${DateTime.now().toString("HH:mm:ss")}],  产生第 $i 条消息")
        val record = new ProducerRecord("topic-B", i.toString, (i*100).toString)

        /** 这个`Future`是Java中的interface - `java.util.concurrent.Future` interface
          * 不是Scala中的trait - `scala.concurrent.Future`
          */
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
