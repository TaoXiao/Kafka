package cn.gridx.kafka.apis.scala.producer

import java.lang.management.ManagementFactory
import java.util.Properties
import org.joda.time.DateTime
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

/**
 * Created by tao on 6/29/15.
 *
 * Producer在send一个message时可以指定callback函数
 *
 */
object SendCallbacks  {

    def main(args: Array[String]): Unit = {
        TestCallbackException()
    }

    /**
     * 测试 `send` 方法的callback method
     */
    def TestCallback() {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ecs1:9092,ecs2:9092")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        val producer = new KafkaProducer[String, String](props)

        for (i <- Range(0, 10)) {
            println(s"调用时间: [${DateTime.now().toString("HH:mm:ss")}],  产生第 $i 条消息")
            val record = new ProducerRecord("topic-B", i.toString, (i * 100).toString)

            /**
             * 所有的call back 方法都必须实现接口`org.apache.kafka.clients.producer.Callback`
             * 当发往server的消息被consumer响应后，`onCompletion`将被调用一次。
             *
             * 注意：该方法将在后台I/O线程中执行，所以它必须很快执行完
             */
            producer.send(record,
                new Callback {
                    override
                    def onCompletion(metadata: RecordMetadata, ex: Exception): Unit = {
                        if (ex != null) /** `metadata`和`ex` 有且只有一个 non-null  */
                            throw ex
                        else {
                            println(s"调用时间：[${DateTime.now().toString("HH:mm:ss")}],  " +
                                    s"Topic: ${metadata.topic()},  Partition: ${metadata.partition()},  " +
                                    s"Offset: ${metadata.offset()},  " +
                                    s"Key: ${record.key()},  Value: ${record.value()}, i : $i")
                            Thread.sleep(3000) // `onComplete`与`send`是异步的，这里的sleep不会阻塞`send`的执行
                        }
                    }
                })
        }
        producer.close()
    }


    /**
     * 测试：如果`send` callback 中出现异常，会怎么样？
     * 测试发现：callback的异常不会抛出到主进程，
     *
     * 所以问题是：怎样才能知道callback中发生了异常？？
     */
    def TestCallbackException(): Unit = {

        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ecs1:9092,ecs2:9092")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        val producer = new KafkaProducer[String, String](props)

        for (i <- Range(0, 4)) {
            println(s"执行时间: [${DateTime.now().toString("HH:mm:ss")}], " +
                    s" 产生第 $i 条消息")
            val record = new ProducerRecord("topic-B", i.toString, (i * 100).toString)
            producer.send(record,
                new Callback {
                    override
                    def onCompletion(metadata: RecordMetadata, ex: Exception): Unit = {
                        if (ex != null)  /** `metadata`和`ex` 有且只有一个 non-null  */
                            throw ex
                        else {
                            Thread.sleep(3000)
                            print(s"\n调用时间：[${DateTime.now().toString("HH:mm:ss")}], i = $i,   ")

                            try { /** 必须自己捕获并处理异常，否则发生的异常信息只会被打印到log中 */
                                val result = i / 0 // 这里制造 divide by zero 异常
                                print(s" 结果为 $result ")
                            } catch {
                                case ex: Exception => { Console.err.println(s"这里发生了异常 - ${ex.getMessage}" ) }
                            }

                        }
                    }
                })
        }
        producer.close()

    }


    /**
      * 测试主线程与Callback的关系，包括它们各自的：
     *   1) process id
     *   2) thread id
      */

    def TestRelationWtihCallback(): Unit = {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ecs1:9092,ecs2:9092")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        val producer = new KafkaProducer[String, String](props)

        /** 主进程的 process id  和  主线程的 thread id */
        val mainProcessId = ManagementFactory.getRuntimeMXBean.getName.split("@")(0)
        val mainThreadId  = Thread.currentThread().getId

        for (i <- Range(0, 10)) {
            println(s"执行时间: [${DateTime.now().toString("HH:mm:ss")}],  " +
                    s"进程ID: $mainProcessId,  线程ID: $mainThreadId,  " +
                    s" 产生第 $i 条消息")
            val record = new ProducerRecord("topic-B", i.toString, (i * 100).toString)
            producer.send(record,
                new Callback {
                    override
                    def onCompletion(metadata: RecordMetadata, ex: Exception): Unit = {

                        /** callback进程的 process id  和  thread id */
                        val thisProcessId = ManagementFactory.getRuntimeMXBean.getName.split("@")(0)
                        val thisThreadId  = Thread.currentThread().getId

                        if (ex != null)  /** `metadata`和`ex` 有且只有一个 non-null  */
                            throw ex
                        else {
                            Thread.sleep(3000)
                            print(s"\n调用时间：[${DateTime.now().toString("HH:mm:ss")}], " +
                                  s"进程ID: $thisProcessId,  线程ID: $thisThreadId,  " +
                                  s"i = $i,   ")
                            val result = i/0  // 这里制造 divide by zero 异常
                            print(s" 结果为 $result ")
                        }
                    }
                })
        }
        producer.close()
    }


}



