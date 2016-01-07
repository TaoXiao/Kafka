package cn.gridx.kafka.apis.scala.admin

import cn.gridx.kafka.apis.scala.Common
import kafka.admin.AdminUtils
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient

/**
 * Created by tao on 6/27/15.
 */
object TopicCommands {
    def main(args: Array[String]): Unit = {
        createTopicExample()
    }

    /**
     * 用API来创建topic
     */
    def createTopicExample(): Unit = {
        val zkClient = new ZkClient(Common.ZK_CONN)

        /**
         * 必须要设置`ZkSerializer`
         * 如果不设置的话，创建的API是显示成功的，但是当用`describe`来查看该topic时，会报错：
         *
         * [root@ecs2 tao]# kafka-topics --zookeeper ecs2:2181 --describe --topic my-3rd-topic
            Error while executing topic command next on empty iterator
            java.util.NoSuchElementException: next on empty iterator
                at scala.collection.Iterator$$anon$2.next(Iterator.scala:39)
                at scala.collection.Iterator$$anon$2.next(Iterator.scala:37)
                at scala.collection.IterableLike$class.head(IterableLike.scala:91)
                at scala.collection.AbstractIterable.head(Iterable.scala:54)
                at kafka.admin.TopicCommand$$anonfun$describeTopic$1.apply(TopicCommand.scala:171)
                at kafka.admin.TopicCommand$$anonfun$describeTopic$1.apply(TopicCommand.scala:161)
                at scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:59)
                at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:47)
                at kafka.admin.TopicCommand$.describeTopic(TopicCommand.scala:161)
                at kafka.admin.TopicCommand$.main(TopicCommand.scala:61)
                at kafka.admin.TopicCommand.main(TopicCommand.scala)
         */
        zkClient.setZkSerializer(ZKStringSerializer)

        val topic = "my-3rd-topic"
        val partitions = 3
        val replicationFactor = 3

        AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor)
    }
}
