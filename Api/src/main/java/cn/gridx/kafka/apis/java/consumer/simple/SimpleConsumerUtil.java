package cn.gridx.kafka.apis.java.consumer.simple;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by tao on 1/8/16.
 *
 * 这是一个 Simple Consumer
 */
public class SimpleConsumerUtil {
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
        /** 连接到不同的broker时，需要对该broker创建新的`SimpleConsumer`实例 */
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
          consumer.close(); /** 关闭连接到该broker的`SimpleConsumer`*/
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
  getLastOffset(String leaderHost, int port, String clientId,
                String topic, int partition, long time) {
    /** 用leader host  创建一个SimpleConsumer */
    SimpleConsumer consumer =
        new SimpleConsumer(leaderHost, port, 100*1000, 64*1024, clientId);

    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> reqInfo = new HashMap<>();
    reqInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(time, 1));
    OffsetRequest req =
        new OffsetRequest(reqInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId);
    OffsetResponse resp = consumer.getOffsetsBefore(req);

    consumer.close();

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
  findNewLeader(String oldLeader, List<String> replicaBrokers, int port,
                String topic, int partition) {
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


  /**
   * 从Kafka中读取消息
   * 这个函数中，我们假定读取的消息的key是Int, value是String
   * 每收到一条消息，会将它的内容在控制台上显示出来
   * */
  public void
  readData(List<String> seedBrokers, int port, String clientId,
           String topic, int partition, long reqOffset, int fetchSize) {
    System.out.println("[Info]: 开始读取数据\n");

    /** 首先查询(topic, partition)的leader broker */
    PartitionMetadata parMeta = findLeader(seedBrokers, port, topic, partition);
    String leaderHost = parMeta.leader().host();
    List<Broker> replicaBrokers = parMeta.replicas();

    /** 为这个leader partition 创建一个SimpleConsumer */
    SimpleConsumer consumer =
        new SimpleConsumer(leaderHost, port, 100*1000, 64*1024, clientId);

    /**
     * 处理错误响应，最多重试5次
     * **/
    FetchResponse resp;
    int numErrors = 0;

    while (true)  {
      /** 创建FetchRequest，并利用SimpleConsumer获取FetchResponse */
      resp = consumer.fetch(
          new FetchRequestBuilder()
                .clientId(clientId)
                .addFetch(topic, partition, reqOffset, fetchSize)
                .build());

      if (resp.hasError()) {
        numErrors++;
        short errorCode = resp.errorCode(topic, partition);
        String errorMsg = ErrorMapping.exceptionFor(errorCode).toString();
        System.err.println("[Error]: 请求获取消息时出现错误: " +
            "\n\t目标lead broker为 " + consumer.host() +
            ", 错误代码为 " + errorCode +
            ", 错误原因为 " + errorMsg);

        if (5 == numErrors) {
          System.err.println("错误次数达到5，不再重试，退出！");
          consumer.close();
          return;
        }

        /** 这种错误不需要重新寻找leader partition */
        if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
          reqOffset = getLastOffset(leaderHost, port, clientId,
              topic, partition, kafka.api.OffsetRequest.LatestTime());
          System.err.println(
              "[Error]: request offset 超出合法范围，自动调整为" + reqOffset);
          continue; //  用新的offset重试
        }
        /** 用新的leader broker来创建SimpleConsumer  */
        else {
          List<String> replicaHosts = new ArrayList<String>();
          for (Broker broker : replicaBrokers)
            replicaHosts.add(broker.host());
          leaderHost = findNewLeader(leaderHost, replicaHosts,
              port, topic, partition).host();
          /**
           * 用新的leader来创建一个SimpleConsumer
           * */
          consumer.close();
          consumer =
              new SimpleConsumer(leaderHost, port, 100*1000, 64*1024, clientId);
        }
      } else {
        break; /** 没有发生错误则直接跳出循环 */
      }
    } // End  => while (true)

    consumer.close();

    /**
     * 开始真正地读取消息
     */
    long numRead = 0;
    for (MessageAndOffset data : resp.messageSet(topic, partition)) {
      /** 要确保：实际读出的offset 不小于 要求读取的offset。
       *  因为如果Kafka对消息进行压缩，那么fetch request将会返回whole compressed block，
       *  即使我们要求的offset不是该 whole compressed block的起始offset。
       *  这可能会造成读取到之前已经读取过的消息。
       *  */
      long currentOffset = data.offset();
      if (currentOffset < reqOffset) {
        System.err.println("[Error]: Current offset = " + currentOffset +
          ",  Requested offset = " + reqOffset + ", Skip. ");
        continue;
      }

      /** `nextOffset` 向最后被读取的消息发起询问：“下一个offset的值是什么？” */
      long nextOffset = data.nextOffset();

      /** Message结构中包含: bytes, key, codec, payloadOffset, payloadSize */
      Message msg = data.message();
      ByteBuffer keyBuf = msg.key();
      byte[] key = new byte[keyBuf.limit()];
      keyBuf.get(key);

      ByteBuffer payload = msg.payload();
      byte[] value = new byte[payload.limit()];
      payload.get(value);

      System.out.printf("消息$%-2d , offset=%-2d , nextOffset=%-2d" +
          " , key=%-2d , value=%s \n",
          numRead+1, currentOffset, nextOffset, bytes2Int(key), bytes2Str(value));

      numRead++;
    }

    System.out.println("[INFO]: 读取完毕，共读取了 " + numRead + " 条消息");
  }

  /**
   * function: byte[] -> int
   * */
  private int bytes2Int(byte[] bytes) {
    if (null == bytes || bytes.length != 4)
      throw new IllegalArgumentException("无法将byte[]转换为int");
    else
      return java.nio.ByteBuffer.wrap(bytes).getInt();
  }

  /**
   * function: byte[] -> String
   * */
  private String bytes2Str(byte[] bytes) {
    if (null == bytes || bytes.length < 1)
      throw new IllegalArgumentException("无法将byte[]转为String");
    else
      return new String(bytes);
  }
}
