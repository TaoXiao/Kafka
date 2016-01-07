package cn.gridx.kafka.apis.java.consumer.highlevel;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

/**
 * Created by tao on 6/24/15.
 * 这是一个简单的High Level Consumer Client
 */

public class ConsumerA implements  Runnable {
  public String title;
  public KafkaStream<Integer, String> stream;

  public ConsumerA(String title, KafkaStream<Integer, String> stream) {
    this.title  = title;
    this.stream = stream;
  }

  @Override
  public void run() {
    System.out.println("开始运行 " + title);

    /** 由于在调用 `ConsumerConnector#createMessageStreams`
     *  已经提供了key和value的decoder，因此这里收到是解码后的数据，
     *  而不是原始的 byte[]
     *  */
    ConsumerIterator<Integer, String> it = stream.iterator();
    /**
     * 不停地从stream读取新到来的消息，在等待新的消息时，hasNext()会阻塞
     * 如果调用 `ConsumerConnector#shutdown`，那么`hasNext`会返回false
     * */
    while (it.hasNext()) {
      MessageAndMetadata<Integer, String> data = it.next();
      String  topic      = data.topic();
      int     partition  = data.partition();
      long    offset     = data.offset();
      int     key        = data.key();
      String  msg        = data.message();

      System.out.println(String.format(
          "Consumer: [%s],  Topic: [%s],  PartitionId: [%d],  Offset: [%d],  Key: [%d], msg: [%s]" ,
          title, topic, partition, offset, key, msg));
    }

    System.err.println(String.format("Consumer: [%s] exiting ...", title));
  }

  /** 从bytes 转换到int */
  public int bytes2Int(byte[] bytes) {
    if (null == bytes)
      throw new IllegalArgumentException("invalid byte array, bytes is null");
    if (bytes.length != 4)
      throw new IllegalArgumentException("invalid byte array, bytes' lenght is " + bytes.length);

    return java.nio.ByteBuffer.wrap(bytes).getInt();
  }
}
