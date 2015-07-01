package cn.gridx.kafka.apis.scala.producer

import java.util.Properties

import cn.gridx.kafka.apis.scala.Common
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

/**
 * Created by tao on 6/29/15.
 *
 * Producer在send一个message时可以指定callback函数
 *
 */
object SendCallbacks {
    /**
     * producer instance
     *
     * 产生<k,v>类型的消息
     */
    def produceKeyedMessages(): Unit = {
        val props = new Properties()

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Common.BROKER_LIST)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

        val producer = new KafkaProducer[String, String](props)

        for (i <- 0 to 10) {
            val record = new ProducerRecord("Topic", i.toString, "val_" + i.toString)
            producer.send(record, new Callback{
                def onCompletion(metadata:RecordMetadata, ex:Exception): Unit = {
                    println(s"partition ---->  ${metadata.partition}")
                }
            })
        }

        producer.close

        println("消息产生结束了")
    }
}
