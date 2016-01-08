package cn.gridx.kafka.apis.java.consumer.simple;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tao on 1/8/16.
 *
 * 这是一个 Simple Consumer
 */
public class ConsumerB {
  /**
   * 为给定的(topic, partition)找到它的leader broker
   * seedBrokers并不一定要是全部的brokers，只要包含一个live broker，
   * 通过这个live broker去查询Leader broker的元数据即可
   * */
  public PartitionMetadata
  findLeader(List<String> seedBrokers, int port, String topic, int partition) {

    PartitionMetadata retMeta = null;

    /** 循环地查询 */
    loop:
    for (String broker : seedBrokers) { /** 按照每一个 broker 循环*/
      SimpleConsumer consumer = null;

      try {
        consumer = new SimpleConsumer(broker, port, 100*1000, 64*1024,
            "leaderLookupClient");
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);

        /** Fetch metadata for a sequence of topics, which
         *  returns metadata for each  topic in the request */
        TopicMetadataResponse resp = consumer.send(req);

        /** 在某个broker上按照每一个topic循环，并检查一个topic中的每一个partition */
        for (TopicMetadata topicMeta : resp.topicsMetadata()) {
          for (PartitionMetadata parMeta : topicMeta.partitionsMetadata()) {
            if (parMeta.partitionId() == partition) {
              retMeta = parMeta; /** 根据 partition id 进行匹配，找到了就退出 */
              break loop;
            }
          }
        }
      } catch (Exception ex) {
        System.err.println("Error: 在向Broker “" + broker +
            "” 询问 (Topic=“" + topic + "”, Partition=“" + partition
            + "”) 的Leader Broker 时发生异常，原因为: \n" + ex.toString());
      } finally {
        if (null != consumer)
          consumer.close();
      }
    } // end => for (String broker : seedBrokers)

    return retMeta;
  }


  /**
   * 确定从(topic, partition)的什么地方开始读取消息，即确定offset
   * Kafka提供了2个常量：
   *    `kafka.api.OffsetRequest.EarliestTime()`找到log data的开始时间
   *    `kafka.api.OffsetRequest.LatestTime()`是最新的时间
   *
   * 最旧的offset并不一定是0，因为随着时间的推移，部分数据将被从log中移除
   *
   *
   * 这里传入的参数`consumer`的host必须是(topic, partition)的leader，
   * 否则会报错，错误代码为6
   *
   * 各种失败代码的含义可以查询这里：kafka.common.ErrorMapping
   * */
  public long
  getLastOffset(SimpleConsumer consumer, String topic, int partition,
                long time, String clientName) {

    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> reqInfo = new HashMap<>();
    reqInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(time, 1));
    OffsetRequest req =
        new OffsetRequest(reqInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
    OffsetResponse resp = consumer.getOffsetsBefore(req);

    /** 处理失败 */
    if (resp.hasError()) {
      short errorCode = resp.errorCode(topic, partition);
      System.err.println("为 (Topic=“" + topic + "”, Partition=“" + partition +
        "”, Time=“" + time + "”) 查询 offset 失败，失败代码为：" + errorCode +
          ",  失败原因为: " + ErrorMapping.exceptionFor(errorCode));
      return -1;
    }

    /** 为什么这里返回的是数组？数组中会有几个数据？
     *  经过实际的测试，里面只有1个数据 */
    long[] offsets = resp.offsets(topic, partition);
    return offsets[0];
  }


  /**
   * 当原来的leader崩溃后，找出新的leader
   * */
  public Broker
  findNewLeader(String oldLeader, List<String> replicaBrokers, int port, String topic, int partition) {
    /**
     * 最多尝试5次，如果还找不到则查询new leader失败
     * */
    for (int loop = 0; loop < 5; loop++) {
      boolean willSleep = false;
      PartitionMetadata parMeta = findLeader(replicaBrokers, port, topic, partition);
      if (null == parMeta || null == parMeta.leader())
        willSleep = true;
      /**
       * 如果leader broker 崩溃，ZooKeeper会探测到这个情况的发生，并重新分配一个new leader broker
       *  这个过程需要很短的时间
       *  如果在第一次循环中，发现 new leader broker 等于 old leader broker，
       *  则睡眠1秒钟给ZK进行调整，并重新尝试查询new leader
       *  **/
      else if (oldLeader.equalsIgnoreCase(parMeta.leader().host()) && 0 == loop) {
        willSleep = true;
      } else {
        return parMeta.leader();
      }

      /**
       * 睡眠1秒钟，给Kafka和ZooKeeper进行调整(failover)等
       * */
      if (willSleep)
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
    }

    // 查询失败
    return null;
  }
}
