package cn.gridx.kafka.apis.scala.consumer

import java.util.Properties

import cn.gridx.kafka.apis.scala.Common
import cn.gridx.kafka.apis.scala.serialization.IntegerSerializer
import kafka.consumer.{ConsumerIterator, KafkaStream, Consumer}
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.Map
import scala.collection.mutable.HashMap

import scala.concurrent.{Future, future}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by tao on 6/25/15.
 */
object MultiThreadConsumer {
    def TOPIC       = "my-3nd-topic"
    def GROUP_ID    = "xt-group-1"


    def main(args: Array[String]): Unit = {
        startProducer()

        Thread.sleep(2000)
        println("producer结束，consumers开始")

        startMultiConsumers()
    }


    def startProducer(): Unit = {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ecs2:9092,ecs3:9092,ecs4:9092")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

        val producer = new KafkaProducer[String, String](props)

        for (i <- 0 to 9) {
            val record = new ProducerRecord(TOPIC, i.toString, "值-" + i)
            producer.send(record)
        }

        producer.close

    }

    /**
     * 一个consumer为一个topic创建多个messageStreams有什么用？
     */
    def startMultiConsumers(): Unit = {
        val connector = Consumer.create(Common.createConsumerConfig(GROUP_ID))

        val topicCountMap = new HashMap[String, Int]()
        topicCountMap.put(TOPIC, 2) // TOPIC在创建时就指定了它有2个partition

        val topicStreamsMap: Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]]
            = connector.createMessageStreams(topicCountMap)

        val streams: List[KafkaStream[Array[Byte], Array[Byte]]] = topicStreamsMap.get(TOPIC).get

        println("# of streams = " + streams.size)

        // 变量futureIndex用来输出Future的序号
        var futureIndex = 0

        /** 消息的总体在两个streams之间怎样分配？？
          * 只会有1个stream中有消息，因为这个consumer只有1个GroupId
          */
        for (stream <- streams) {
            processSingleStream(futureIndex, stream)
            futureIndex = futureIndex+1
        }

        Thread.sleep(5000)
        /* 注意，这里虽然主线程退出了，但是已经创建的各个Future任务仍在运行（一直在等待接收消息）
         * 怎样在主线程里结束各个Future任务呢？
         */

        println("consumer结束，全部退出！")
    }

    /**
     * 一个Future处理一个stream
     * TODO:  还需要一个可以控制Future结束的机制
     * @param futureIndex
     * @param stream
     * @return
     */
    def processSingleStream(futureIndex:Int, stream: KafkaStream[Array[Byte], Array[Byte]]): Future[Unit] = future {
        val it: ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator()
        while (it.hasNext) {
            val data: MessageAndMetadata[Array[Byte], Array[Byte]] = it.next()
            println("futureNumer->[" + futureIndex + "],  key->[" + new String(data.key) + "],  message->[" + new String(data.message) + "],  partition->[" +
                    data.partition + "],  offset->[" + data.offset + "]")
        }
    }
}
