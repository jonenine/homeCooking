package mx.homeCooking;

import org.junit.Test;

import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerGroupTest {

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

    @Test
    public void test1() {
        try {
            AbstractExecutorService tg = WorkerGroups.executor("test", 12);

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

    public void testShutDown() {
        while(true){
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

    public static void main(String[] args) {
        WorkerGroupTest test = new WorkerGroupTest();
        test.test1();
    }

}
