package mx.homeCooking.workGroup;

import org.junit.Test;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerGroupTest {

    static CountDownLatch cdl = new CountDownLatch(1);
    static void sleep(long l) {
        try {
            cdl.await(l,TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static long time() {
        return System.currentTimeMillis();
    }

    static long time(long start) {
        return System.currentTimeMillis() - start;
    }


    static void test10(ExecutorService tg) throws InterruptedException {
        final Queue<String> queue = new LinkedBlockingDeque<>();

        final int sum = 3000000;
        final int producerSum = 10;
        final CountDownLatch cdh = new CountDownLatch(producerSum);

        for (int j = 0; j < producerSum; j++) {
            new Thread(() -> {
                final AtomicInteger counter = new AtomicInteger(0);
                final long start = System.currentTimeMillis();
                for (int i = 0; i < sum; i++) {
                    final int ii = i;
                    tg.execute(() -> {
                        //queue.add("哈哈:" + ii + " " + Thread.currentThread().getName());
                        int _count = counter.incrementAndGet();
                        if (_count == sum) {
                            System.err.println("消费时间:" + (System.currentTimeMillis() - start));
                            cdh.countDown();
                        } else if (_count > sum) {
                            System.err.println("消费错误----------------------------------");
                        }
                    });
                }
                System.err.println("入队时间:" + (System.currentTimeMillis() - start));
            }).start();
        }

        cdh.await();
        int size = queue.size();
        String line;
        while ((line = queue.poll()) != null) {
            //System.out.println(line);
        }
        System.err.println("共接受消息" + size + "条");
    }

    public void testShutDown() {
        while (true) {
            try {
                WorkerGroup tg = (WorkerGroup) WorkerGroups.executor("test", 12);

                for (int i = 0; i < 1; i++) {
                    test10(tg);
                    System.err.println("第" + i + "批次完成");
                    //Thread.sleep(7000);
                }
                tg.shutdown();
                System.err.println("shutdown");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testPerformance() {
        try {
            //AbstractExecutorService tg = WorkerGroups.executor("test", 8);
            //WorkerGroup tg = WorkerGroups.timeoutExecutor("test", 8);
            AbstractExecutorService tg = WorkerGroups.scheduledExecutor("test", 8);
            //ExecutorService tg = Executors.newFixedThreadPool(8);
            //ExecutorService tg = Executors.newScheduledThreadPool(8);
            //ExecutorService tg = new ForkJoinPool(16);

            long start = System.currentTimeMillis();
            for (int i = 0; i < 20; i++) {
                test10(tg);
                System.err.println("第" + i + "批次完成");
                //Thread.sleep(7000);
            }
            System.err.println("总耗时" + (System.currentTimeMillis() - start));
            tg.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        WorkerGroupTest test = new WorkerGroupTest();
        test.testPerformance();
    }


    @Test
    public void watchLifeCycle() {
        WorkerGroup tg = (WorkerGroup) WorkerGroups.executor("test", 8);
        sleep(2000);

        while(true){
            System.err.println("开始消费:");
            CountDownLatch cdl = new CountDownLatch(1);
            int sum = 10000;
            long start = time();
            AtomicInteger counter = new AtomicInteger(0);
            for (int i = 0; i < sum; i++) {
                /**
                 * 单线程入队延时任务,看看任务能不能重平均
                 */
                tg.execute(() -> {
                    sleep(1);
                    int count;
                    if ((count = counter.incrementAndGet()) == sum) {
                        System.err.println("消费结束:" + time(start));
                        cdl.countDown();
                    }else if(count>sum){
                        System.err.println("消费异常 count:" + count);
                    }
                });
            }
            System.err.println("入队结束:" + time(start)+",线程池队列大小:"+tg.getQueueSize());
            try {
                cdl.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.err.println("线程池队列大小:"+tg.getQueueSize());
            sleep(10000);
            System.err.println("线程池队列大小:"+tg.getQueueSize());
        }


    }

}
