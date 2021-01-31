package mx.homeCooking.workGroup;

import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WorkerGroupTest {

    static CountDownLatch cdl = new CountDownLatch(1);

    static void sleep(long l) {
        try {
            cdl.await(l, TimeUnit.MILLISECONDS);
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

    /**
     * @param producerSum 入队线程数
     * @param tg
     * @return
     * @throws InterruptedException
     */
    static float[] test10(int producerSum , ExecutorService tg) throws InterruptedException {
        /**
         * 每入队线程入队任务数
         */
        final int sum = 10000000;

        final CountDownLatch cdh = new CountDownLatch(producerSum);

        final AtomicLong enqueueTimeSum = new AtomicLong(0);
        final AtomicLong consumeTimeSum = new AtomicLong(0);

        for (int j = 0; j < producerSum; j++) {
            new Thread(() -> {
                final AtomicInteger counter = new AtomicInteger(0);
                final long start = System.currentTimeMillis();
                for (int i = 0; i < sum; i++) {
                    final int ii = i;
                    tg.execute(() -> {
                        int _count = counter.incrementAndGet();
                        if (_count == sum) {
                            long consumeTime = System.currentTimeMillis() - start;
                            consumeTimeSum.addAndGet(consumeTime);
                            System.err.println("消费时间:" + consumeTime);
                            cdh.countDown();
                        } else if (_count > sum) {
                            System.err.println("消费错误----------------------------------");
                        }
                    });
                }
                long consumeTime = System.currentTimeMillis() - start;
                enqueueTimeSum.addAndGet(consumeTime);
                System.err.println("入队时间:" + consumeTime);
            }).start();
        }

        cdh.await();

        float avgEnqueue = enqueueTimeSum.get() / new Float(producerSum);
        float avgConsume = consumeTimeSum.get() / new Float(producerSum);

        System.err.println("平均入队时间:" + avgEnqueue + "/平均消费时间:" + avgConsume);

        return new float[]{avgEnqueue, avgConsume};
    }

    public void testShutDown() {
        while (true) {
            try {
                WorkerGroup tg = (WorkerGroup) WorkerGroups.executor("test", 12);
                for (int i = 0; i < 1; i++) {
                    test10(8,tg);
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

    static ExecutorService createExecutorService() {
        //AbstractExecutorService tg = WorkerGroups.executor("test", 8);
        //WorkerGroup tg = WorkerGroups.timeoutExecutor("test", 8);
        AbstractExecutorService tg = WorkerGroups.scheduledExecutor("test", 8);
        //ExecutorService tg = Executors.newFixedThreadPool(8);
        //ExecutorService tg = Executors.newScheduledThreadPool(8);
        //ExecutorService tg = new ForkJoinPool(8);

        return tg;
    }

    /**
     * 多线程入队测试
     */
    @Test
    public void testPerformance(int threadNum) {
        try {
            ExecutorService tg = createExecutorService();
            long start = System.currentTimeMillis();
            float enqueueTimeSum = 0;
            float consumeTimeSum = 0;
            for (int i = 0; i < 20; i++) {
                float[] vs = test10(threadNum,tg);
                System.err.println("第" + i + "批次完成");
                //Thread.sleep(7000);
                if (i >= 10) {//10-19用来计算平均时间
                    enqueueTimeSum += vs[0];
                    consumeTimeSum += vs[1];
                }
            }
            System.err.println("总耗时" + (System.currentTimeMillis() - start)+",平均入队时间:"+enqueueTimeSum/10+"平均消费时间:"+consumeTimeSum/10);
            tg.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        WorkerGroupTest test = new WorkerGroupTest();
        test.testPerformance(1);
    }


    @Test
    public void watchLifeCycle() {
        WorkerGroup tg = (WorkerGroup) WorkerGroups.executor("test", 8);
        sleep(2000);

        while (true) {
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
                    } else if (count > sum) {
                        System.err.println("消费异常 count:" + count);
                    }
                });
            }
            System.err.println("入队结束:" + time(start) + ",线程池队列大小:" + tg.getQueueSize());
            try {
                cdl.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.err.println("线程池队列大小:" + tg.getQueueSize());
            sleep(10000);
            System.err.println("线程池队列大小:" + tg.getQueueSize());
        }


    }

}
