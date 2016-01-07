package cn.gridx.kafka.apis.scala.pitfalls

import java.util.Properties

import cn.gridx.kafka.apis.scala.Common
import kafka.consumer.{ConsumerConfig, Consumer}

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by tao on 6/27/15.
 */
object FutureCanNotExit {
    def TOPIC = "my-3rd-topic"

    def main(args: Array[String]) = {
        val consumerConfig = createConfig("group-disable-autocommit")
        val connector = Consumer.create(consumerConfig)

        if (consumerConfig.autoCommitEnable) println("auto.commit.enable = true")
        else println("auto.commit.enable = false")

        val topicCountMap = Map(TOPIC -> 1)

        /**
         * 诡异！！！
         * 当`main`函数不调用`System.exit`且调用了`connector.createMessageStreams`时，
         * 即使主线程退出了，Future还会继续运行(Future做的事跟Kafka没有任何关系)
         * 即进程不会结束。为什么？
         *
         * 因为`connector.createMessageStreams(topicCountMap)`会导致产生永不停止的后台进程（在监听和等待新的消息），
         * 造成主进程无法结束。
         *
         * 只要在`main`函数中加上`System.exit()`就可以立即结束主进程
         */
        connector.createMessageStreams(topicCountMap)

        newThread()

        println("Main Thread is exiting ...")

        // 如果调用了exit，那么整个进程就会结束
        //System.exit(1)
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

    def newThread(): Future[Unit] = future {
        var i = 0
        while (true) {
            println(s"From the new thread,  i = $i")
            i += 1
            Thread.sleep(1000)
        }
    }
}
