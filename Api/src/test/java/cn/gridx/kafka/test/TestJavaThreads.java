package cn.gridx.kafka.test;

import  java.lang.System.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Created by tao on 6/25/15.
 */
public class TestJavaThreads {

    public static void main(String[] args) {
        System.out.println("start");
        Executor executor = Executors.newFixedThreadPool(3);
        executor.execute(new MyApp(1));
        executor.execute(new MyApp(2));
        executor.execute(new MyApp(3));
        executor.execute(new MyApp(4));
        executor.execute(new MyApp(5));
        System.out.println("finish");
    }

    static public class MyApp implements Runnable {
        private int num ;
        private int count = 0;

        public MyApp(int num) {
            this.num = num;

        }

        public void run() {
            try {
                while (true) {
                    Thread.sleep(1000);
                    System.out.println("Thread #" + num);
                    if (count++ > 4)
                        break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
