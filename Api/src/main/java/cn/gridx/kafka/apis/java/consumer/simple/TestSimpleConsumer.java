package cn.gridx.kafka.apis.java.consumer.simple;

import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by tao on 1/8/16.
 */
public class TestSimpleConsumer {
  public static void main(String[] args) {
    // TestFindLeader();
    // TestGetLastOffset();
    //TestFindNewLeader();
    TestReadData(args[0], Integer.parseInt(args[1]),  Long.parseLong(args[2]), Integer.parseInt(args[3]));
  }




  /**
   * 测试`ConsumerB#findLeader`方法的使用
   * 找到(Topic=“topic-C”, Partition=0)当前的leader partition
   * */
  public static void TestFindLeader() {
    SimpleConsumerUtil consumer = new SimpleConsumerUtil();
    List<String> seedBrokers = new ArrayList<>();
    seedBrokers.add("ecs1.njzd.com");
    seedBrokers.add("ecs3.njzd.com");
    PartitionMetadata parMeta = consumer.findLeader(seedBrokers, 9092, "topic-C", 0);
    System.out.println("Partition metadata for (Topic=“topic-C”, Partition=“0”) is : \n"
        + parMeta + "\n");
  }



  /***
   * 测试`ConsumerB#getLastOffset`方法的使用
   * 找出(Topic=“topic-C”, Partition=1)的最老、最新、当前的offset
   * */
  public static void TestGetLastOffset() {
    List<String> brokers = new ArrayList<>();
    brokers.add("ecs1");
    brokers.add("ecs4");
    int port = 9092;
    String topic  = "topic-C";
    int partition = 1;

    /**
     * 首先查询出(Topic=“topic-C”, Partition=“1”)的leader broker's hostname
     * */
    SimpleConsumerUtil util = new SimpleConsumerUtil();
    PartitionMetadata parMeta = util.findLeader(brokers, port, topic, partition);
    String leaderHost = parMeta.leader().host();

    /** 最老的offset（依然在log中的msg）*/
    long earliestOffset = util.getLastOffset(leaderHost, port, "Client - 查询offset",
        topic, partition, OffsetRequest.EarliestTime());
    /** 最新的offset */
    long latestOffset = util.getLastOffset(leaderHost, port, "Client - 查询offset",
        topic, partition, OffsetRequest.LatestTime());
    /** 当前的offset */
    long currentOffset = util.getLastOffset(leaderHost, port, "Client - 查询offset",
        topic, partition, System.currentTimeMillis());

    System.out.println("(Topic=“" + topic + "”, Partition=“" + partition +
        "”) 的leader host是" + leaderHost);
    System.out.println("(Topic=“" + topic + "”, Partition=“" + partition +
        "”) 中的offsets: " +
        "\n\tEarliest Offset: " + earliestOffset +
        "\n\tLatest   Offset: " + latestOffset +
        "\n\tCurrent  Offset: " + currentOffset);
  }


  /**
   * 测试`ConsumerB#findNewLeader`方法的使用
   *
   * 测试方法：先找出一个leader broker，然后将该broker关掉，
   *          再使用`ConsumerB#findNewLeader`方法找出新的leader，
   *          检查能否正确地找出新leader
   *
   * */

  public static void TestFindNewLeader() {
    /**
     * 首先找到(Topic=“topic-C”, Partition=2)的leader broker与replica brokers
     * */
    SimpleConsumerUtil consumer = new SimpleConsumerUtil();
    List<String> seedBrokers = new ArrayList<>();
    seedBrokers.add("ecs1.njzd.com");
    seedBrokers.add("ecs3.njzd.com");
    int port = 9092;
    String topic = "topic-C";
    int partition = 2;

    PartitionMetadata parMeta = consumer.findLeader(seedBrokers, port, topic, partition);
    String leaderHost   = parMeta.leader().host();
    List<Broker> replicas =  parMeta.replicas();
    List<String> replicaHosts = new ArrayList<String>();
    for (Broker broker : replicas)
    replicaHosts.add(broker.host());

    // 打印出当前的leader broker
    System.out.println("leader broker is " + leaderHost +
      "\nreplica brokers are : ");
    for (Broker replica : replicas)
      System.out.println(replica.host() + "  ");

    // 开始120秒倒计时，请杀掉当前的leader broker
    System.out.println("\n我将在60秒后查找新的leader broker，请在此期间杀掉当前的leader broker\n");
    for (int i = 1; i <= 60; i++) {
      System.out.print("\r");
      for (int j = 1; j <= i; j++)
        System.out.print("=");
      System.out.print("> " + i);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    System.out.println("\n我已醒来，正在查找新的leader broker");

    /**
     * 下面开始调用 `findNewLeader` 找出新的leader broker
     * */
    Broker newLeader = consumer.findNewLeader(leaderHost, replicaHosts, port, topic, partition);
    System.out.println("新的leader broker 为 " + newLeader.host());
  }



/**
 * 测试方法 : readData
 *
 * */
public static void TestReadData(String topic, int partition, long reqOffset, int fetchSize) {
  List<String> brokers = new ArrayList<>();
  brokers.add("ecs1");
  brokers.add("ecs4");
  int port = 9092;

  SimpleConsumerUtil util = new SimpleConsumerUtil();
  util.readData(brokers, port, "ClientReadData",
      topic, partition, reqOffset, fetchSize);
}
}
