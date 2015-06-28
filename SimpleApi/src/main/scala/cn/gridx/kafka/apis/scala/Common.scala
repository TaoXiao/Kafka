package cn.gridx.kafka.apis.scala

import java.util.Properties

import kafka.consumer.ConsumerConfig

/**
 * Created by tao on 6/26/15.
 */
object Common {
    def ZK_CONN     = "ecs1:2181,ecs2:2181,ecs3:2181"
    def BROKER_LIST = "ecs1:9092,ecs2:9092"

    /**
     *  结果配置项的意义
     *
     * `zookeeper.connect` : Kafka uses ZK to store offset of messages consumed for
     *         a specific topic and partition by this Consumer Group
     *
     * `group.id` : this string defines the Consumer Group this process is consuming on behalf of
     *
     * `zookeeper.session.timeout.ms` :  how many milliseconds Kafka will wait for ZK to respond to
     *          a read or write request before giving up and continuing to consume messages
     *
     *
     *  `zookeeper.sync.time.ms` : the number of milliseconds a ZK follower can be behind the master
     *          before an error occurs
     *
     *  `auto.commit.interval.ms` : how often updates to the consumed offsets are written to ZK.
     *          Since the commit frequency is time based instead of #messages consumed, if an
     *          error occurs between updates to ZK on restart you will get replayed messages
     *
     * @return
     */
    def createConsumerConfig(groupId: String): ConsumerConfig = {
        val props = new Properties();
        props.put("zookeeper.connect", Common.ZK_CONN)
        props.put("group.id", groupId)
        props.put("zookeeper.session.timeout.ms", "400")
        props.put("zookeeper.sync.time.ms", "200")
        props.put("auto.commit.interval.ms", "1000")

        new ConsumerConfig(props)
    }
}
