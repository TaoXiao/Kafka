package cn.gridx.kafka.apis.scala.consumer

import java.util.Properties

import cn.gridx.kafka.apis.scala.Common
import kafka.consumer.{KafkaStream, Consumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer


import scala.concurrent.{Await, future}
import scala.concurrent.ExecutionContext.Implicits.global


import scala.concurrent.duration._


/**
 * 例子：消费带key的message
 * Created by tao on 6/29/15.
 */
object ConsumeKeyedMessages {
    def Group_Id = "group-keyed-messages"
    def Topic = "my-3rd-topic"

    def main(args: Array[String]): Unit = {
       if (args(0).equals("producer"))  produceKeyedMessages
       else if (args(0).equals("consumer"))  consumeKeyedMessages
       else println("<producer> or <consumer> ?")
    }

    /**
     * consumer instance
     */
    def consumeKeyedMessages(): Unit = {
        val connector = Consumer.create(Common.createConsumerConfig(Group_Id))

        val topicCountMap =  Map(Topic -> 1)
        val topicStreamsMap = connector.createMessageStreams(topicCountMap)
        val streams = topicStreamsMap.get(Topic).get

        val f = processKeyedMsgStream(streams.head)

        Await.result(f, 1000 second)
    }
    

    def processKeyedMsgStream(stream: KafkaStream[Array[Byte], Array[Byte]]) = future {
        val it = stream.iterator

        while (it.hasNext) {
            val data = it.next
            val key =  new String(data.key)
            val message = new String(data.message)
            val partition = data.partition
            val offset = data.offset
            println(s"Key->$key | message->${message}} | partition->${partition}  | offset->${offset}")
        }
    }

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
            val record = new ProducerRecord(Topic, i.toString, "val_" + i.toString)
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
