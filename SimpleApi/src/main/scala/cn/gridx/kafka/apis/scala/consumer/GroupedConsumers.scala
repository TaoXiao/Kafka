package cn.gridx.kafka.apis.scala.consumer

import cn.gridx.kafka.apis.scala.Common
import kafka.consumer.Consumer

import scala.collection.mutable


/**
 * Created by tao on 6/26/15.
 *
 * 测试Consumer Group的行为
 *
 * 简单起见，每个Consumer端只用一个consumer thread
 *
 * 用法：
 *  在两个节点上分别运行本程序，然后向TOPIC发送消息，
 *  如果两个consumer的group_id不同，但是consumer_id相同，则两个group中的consumer都能收到消息
 *  如果两个consumer的group_id相同，则两个group中只有1个consumer能收到消息
 *
 *
 */
object GroupedConsumers {
    def TOPIC = "my-2nd-topic"

    def main(args: Array[String]): Unit = {
        if (args.length != 2) {
            println("Please input <group_id> and <consumer_id>")
            return
        }

        val groupId = args(0)
        val consumerId = args(1)

        println("***********************************")
        println(s"group id    -> $groupId")
        println(s"consumer id -> $consumerId")
        println("***********************************")

        val connector = Consumer.create(Common.createConsumerConfig(groupId))

        val topicCountMap = new mutable.HashMap[String, Int]()
        topicCountMap.put(TOPIC, 1)

        val streamList = connector.createMessageStreams(topicCountMap)
        val stream = streamList.get(TOPIC)

        if (stream.isEmpty || stream.get.isEmpty) {
            println("没有stream，退出！")
            return
        }

        val it = stream.get.head.iterator()

        while (it.hasNext) {
            val data = it.next
            println(new String(data.key) + " -> " + new String(data.message))
        }
    }


}
