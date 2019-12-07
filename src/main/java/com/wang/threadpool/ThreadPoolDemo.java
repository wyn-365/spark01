package com.wang.threadpool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ThreadPoolDemo {
    public static void main(String[] args) {
        //掉用线程池jdk1.5之后

        //创建一个单线程的线程池 会一直运行
        //ExecutorService pool = Executors.newSingleThreadExecutor();

        //创建固定大小的线程池
        //ExecutorService pool = Executors.newFixedThreadPool(5);

        //可缓冲的线程池 可有任意多个线程 根据电脑【双核 四线程】
        ExecutorService pool = Executors.newCachedThreadPool();
        for (int i = 0; i <= 10; i++){
            pool.execute(new Runnable(){
                @Override
                public void run(){
                    //获取当前现成的名字
                    System.out.println(Thread.currentThread().getName());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName()+" is over!");
                }
            });
        }

        //停用线程池
        System.out.println("All task is over!");
        pool.shutdown();

    }
}
