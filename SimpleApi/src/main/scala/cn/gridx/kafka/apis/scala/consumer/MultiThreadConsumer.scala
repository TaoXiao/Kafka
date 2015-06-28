package cn.gridx.kafka.apis.scala.consumer

import cn.gridx.kafka.apis.scala.Common
import kafka.consumer.{ConsumerIterator, KafkaStream, Consumer}
import kafka.message.MessageAndMetadata

import scala.collection.Map
import scala.collection.mutable.HashMap

import scala.concurrent.{Future, future}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by tao on 6/25/15.
 */
object MultiThreadConsumer {
    def TOPIC       = "my-2nd-topic"
    def GROUP_ID    = "xt-group-1"


    /**
     * 另一个producer产生的消息如下：
     *  i=0,  offset=290,  partition=1
        i=1,  offset=225,  partition=0
        i=2,  offset=226,  partition=0
        i=3,  offset=291,  partition=1
        i=4,  offset=292,  partition=1
        i=5,  offset=227,  partition=0
     *
     * 运行结果如下：
     *
     *  开始了
        # of streams is 1
        futureNumer->[0],  key->[key-0],  message->[msg-0],  partition->[1],  offset->[290]
        futureNumer->[1],  key->[key-1],  message->[msg-1],  partition->[0],  offset->[225]
        futureNumer->[1],  key->[key-2],  message->[msg-2],  partition->[0],  offset->[226]
        futureNumer->[0],  key->[key-3],  message->[msg-3],  partition->[1],  offset->[291]
        futureNumer->[0],  key->[key-4],  message->[msg-4],  partition->[1],  offset->[292]
        futureNumer->[1],  key->[key-5],  message->[msg-5],  partition->[0],  offset->[227]
        结束了
     * @param args
     */
    def main(args: Array[String]): Unit = {
        println(" 开始了 ")

        val connector = Consumer.create(Common.createConsumerConfig(GROUP_ID))

        val topicCountMap = new HashMap[String, Int]()
        topicCountMap.put(TOPIC, 2) // TOPIC在创建时就指定了它有2个partition

        val msgStreams: Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]]
                = connector.createMessageStreams(topicCountMap)

        println("# of streams is " + msgStreams.get(TOPIC).get.size)

        // 变量futureIndex用来输出Future的序号
        var futureIndex = 0
        for (stream <- msgStreams.get(TOPIC).get) {
            processSingleStream(futureIndex, stream)
            futureIndex = futureIndex+1
        }

        Thread.sleep(30000)
        /* 注意，这里虽然主线程退出了，但是已经创建的各个Future任务仍在运行（一直在等待接收消息）
         * 怎样在主线程里结束各个Future任务呢？
         */
        println(" 结束了 ")

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
