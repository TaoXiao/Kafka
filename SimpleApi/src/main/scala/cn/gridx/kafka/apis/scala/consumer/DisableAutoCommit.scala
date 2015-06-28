package cn.gridx.kafka.apis.scala.consumer

import java.util.Properties

import cn.gridx.kafka.apis.scala.Common
import kafka.consumer.{KafkaStream, Consumer, ConsumerConfig}

import scala.concurrent.{Await, Future, future}
import scala.concurrent.ExecutionContext.Implicits.global


import scala.concurrent.duration._


/**
 * Created by tao on 6/26/15.
 *
 * 禁用 auto commit，改为由consumer自己来commit offset
 */
object DisableAutoCommit {
    def TOPIC = "my-3rd-topic"

    def main_old(args: Array[String]): Unit = {
        val consumerConfig = createConfig("group-disable-autocommit")
        val connector = Consumer.create(consumerConfig)

        if (consumerConfig.autoCommitEnable) println("auto.commit.enable = true")
        else println("auto.commit.enable = false")

        val topicCountMap = Map(TOPIC -> 1)
        val streamList = connector.createMessageStreams(topicCountMap)

        val totalSum = new TotalSum(0)

        val f = processStream(streamList.get(TOPIC).get.head, totalSum)

        try {
            Await.result(f, 4 second)
        } catch {
            case ex => println(s"捕捉到Await抛出的异常 ${ex.getClass.getCanonicalName}，内容为\n{\n${ex.getMessage}\n}")
        }

        println("结果为 " + totalSum.value)

    }


    def createConfig(groupId: String): ConsumerConfig = {
        val props = new Properties();
        props.put("zookeeper.connect", Common.ZK_CONN)
        props.put("group.id", groupId)
        props.put("zookeeper.session.timeout.ms", "400")
        props.put("zookeeper.sync.time.ms", "200")
        props.put("auto.commit.enable", "false")

        new ConsumerConfig(props)
    }

    /**
     * 将producer产生的消息中的value都累加起来
     *
     * 如果超过5秒没有新的消息，就退出
     *
     * @param stream
     * @return
     */
    def processStream(stream: KafkaStream[Array[Byte], Array[Byte]], sum: TotalSum): Future[Unit] = future {
        val it = stream.iterator()

        while (it.hasNext) {
            sum.value += new String(it.next.message).toInt
        }
    }


    class TotalSum(var value:Int) {

    }


}
