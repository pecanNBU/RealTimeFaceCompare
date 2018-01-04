package demo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 线程池的方式创建线程
 */
public class ThreadPoolTest {
    public static void main(String[] args) throws InterruptedException {
        //1.创建线程池 
//        int cores=Runtime.getRuntime().availableProcessors();
        int cores = 4;
        ExecutorService threadPool = Executors.newFixedThreadPool(cores);
        ThreadPoolDemo demo = new ThreadPoolDemo();
        //2.提交线程任务
        for (int i = 0; i < cores; i++) {
            threadPool.submit(demo);
        }
        //3.结束线程
        threadPool.shutdown();
    }
}

class ThreadPoolDemo implements Runnable {
    public void run() {
        //第一级目录为随机
        UpDataToFtp.upDataTestFirst("E:/test60", 100, "");
        //最后一级目录为随机
//        UpDataToFtp.upDataTestEnd("E:/test60", 100, "");

    }
}