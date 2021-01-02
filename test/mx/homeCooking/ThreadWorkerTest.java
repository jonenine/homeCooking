package mx.homeCooking;

import org.junit.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ThreadWorkerTest {

    public static void main(String[] args) {
        try {
            ThreadWorkerTest tester = new ThreadWorkerTest();
            tester.warmUp();
            for (int i = 0; i < 10; i++) {
                tester.testPerformance();
                System.err.println("第" + i + "批次测试完成!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 热身测试,可以用来提前记载所需的类
     */
    public void warmUp() throws Exception {
        //ExecutorService threadWorker = new ThreadWorker("test0");
        ExecutorService threadWorker = new ScheduledThreadWorker("test1");

        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch cdl = new CountDownLatch(1);
        final int sum = 1000;
        for (int i = 0; i < sum; i++) {
            threadWorker.execute(() -> {
                int count = counter.incrementAndGet();
                if (count == sum) {
                    cdl.countDown();
                } else if (count > sum) {
                    System.err.println("消费个数大于生产个数");
                }
            });
        }

        cdl.await();

        System.out.println("共消费" + counter.get());
        assertEquals(counter.get(), sum);
        System.out.println("warmUp threadWorker已经销毁:" + threadWorker.shutdownNow());
    }

    /**
     * 测试shutDown,以及shutdown后处理遗留任务
     */
    @Test
    public void testShutDown() throws Exception {
        ScheduledThreadWorker threadWorker = new ScheduledThreadWorker("test1");

        final AtomicInteger counter = new AtomicInteger(0);
        final int sum = 1000000;
        for (int i = 0; i < sum; i++) {
            threadWorker.execute(() -> {
                int count = counter.incrementAndGet();
                //降低消费速度
                if (count % 100000 == 1) {
                    try {
                        //Thread.sleep(1);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                if (count == sum) {
                    System.out.println("完成消费" + count + "/" + threadWorker.size());
                } else if (count > sum) {
                    System.err.println("消费个数大于生产个数");
                    System.exit(1);
                }
            });
        }

        try {
            threadWorker.shutdown();
            /**
             * 测试shutdown后添加任务
             */
//            threadWorker.execute(()->{
//                System.err.println("不可能看到我");
//                System.exit(1);
//            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        //已经消费数
        int count = counter.get();
        //剩余任务数
        int remain = threadWorker.size();

        System.out.println("当前消费" + count + "/" + remain + "/" + (count + remain));
        while (threadWorker.getThread().isAlive()) {

        }
        System.err.println(threadWorker.size());
    }

    @Test
    public void testShutDownManyTimes() throws Exception {
        for (int i = 0; i < 1000; i++) {
            testShutDown();
        }

    }


    /**
     * 测试唤醒
     */
    @Test
    public void testAWait() throws Exception {

        int sum = 1000000;
        int notInLastTask = sum - 1;

        for (int i = 0; i < 4; i++) {
            final int ii = i;
            new Thread(new Runnable() {
                ScheduledThreadWorker threadWorker = new ScheduledThreadWorker("test" + ii);
                AtomicInteger batchCount = new AtomicInteger(0);

                @Override
                public void run() {
                    while (true) {
                        AtomicInteger count = new AtomicInteger(0);
                        //1.
                        for (int i = 0; i < notInLastTask; i++) {
                            threadWorker.execute(() -> {
                                count.incrementAndGet();
                            });
                        }
                        //2.
                        try {
                            Thread.sleep(threadWorker.size() % 3);
                            //System.out.println("-----:" + threadWorker.size());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        //3.发最后一个任务,看能否唤醒
                        new Thread(()->{
                            threadWorker.execute(() -> {
                                //此时threadWorker肯定是1,因为这个runnable还没有执行完成呢!
                                System.out.println("最后一个任务消费完毕!");
                            });
                        }).start();

                        System.out.println("-----:" + threadWorker.size());

                        while (threadWorker.size() > 0) {

                        }
                        System.out.println(count.incrementAndGet() + "/" + threadWorker.size());
                        System.err.println(ii + "-" + batchCount.incrementAndGet());

                    }
                }
            }).start();
        }

        try {
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
        }
    }

    /**
     * 测试调度
     */
    @Test
    public void testSchedule() {
        ScheduledThreadWorker threadWorker = new ScheduledThreadWorker("test1");

        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    System.err.println(new Date());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        Random r = new Random();
        int sum = 10000;

        while (true) {
            final AtomicInteger counter = new AtomicInteger(0);
            final BitSet bitset = new BitSet(sum);
            CountDownLatch cdl = new CountDownLatch(1);
            for (int i = 0; i < sum; i++) {
                final int ii = i;
                //时间上也要随机,定时不能总是原来越大,也要相应变小
                final long delay = 10 + ii / 100 - (int) (10 * r.nextFloat());
                final long date = System.currentTimeMillis() + delay;
                threadWorker.schedule(() -> {
                    long late = System.currentTimeMillis() - date;
                    if (Math.abs(late) > 50) {
                        System.err.println("调度误差超过50毫秒:" + late);
                    }
                    int count = counter.incrementAndGet();
                    if (count > sum) {
                        System.err.println("消费个数大于生产个数");
                        System.exit(1);
                    }

                    /**
                     * bitset虽然不是线程安全的,但是
                     * 1.在这个单线程中访问final
                     * 2.最后通过CountDownLatch保证可见性
                     */
                    assertFalse(bitset.get(ii));
                    bitset.set(ii);

                    if (count == sum) {
                        cdl.countDown();
                    }

                }, delay);
            }

            try {
                cdl.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //引入bitset做一个校验
            assertEquals(bitset.nextClearBit(0), sum);

            System.err.print("剩余定时数量:" + threadWorker.getDateSize());
        }
    }

    /**
     * 在低压力下,也就是线程没有100%压满的情况下,不容易测出问题
     * 多线程入队情况下,测试调度
     */
    @Test
    public void testScheduleFromMT() {
        /**
         * 8线程,每队10万,平均入队时间略大于50毫秒
         * 超过20万时,开始出现调度误差
         */
        int productThreadSum = 8;
        int taskSum = 300000;

        ScheduledThreadWorker threadWorker = new ScheduledThreadWorker("test1");
        //ScheduledThreadPoolExecutor threadWorker = new ScheduledThreadPoolExecutor(1);

        CountDownLatch[] allCdl = new CountDownLatch[1];
        AtomicInteger[] counters = new AtomicInteger[productThreadSum];

        //flush thread
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    StringBuilder sb = new StringBuilder();
                    for(AtomicInteger counter:counters){
                        sb.append(counter.get()+",");
                    }
                    //System.err.println(new Date()+"/"+threadWorker.getDateSize()+"---- "+sb);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        while (true) {
            synchronized (allCdl){
                allCdl[0] = new CountDownLatch(productThreadSum);
            }
            for (int j = 0; j < productThreadSum; j++) {
                final AtomicInteger counter = counters[j] = new AtomicInteger(0);
                final AtomicInteger misCounter = new AtomicInteger(0);
                final BitSet bitset = new BitSet(taskSum);
                final CountDownLatch cdl  = new CountDownLatch(1);

                int jj = j;
                /**
                 * 以多线程入同一个队列,多线程之间彼此不知道
                 */
                new Thread(() -> {
                    long start = System.currentTimeMillis();
                    System.out.println(jj+"开始入队");
                    for (int i = 0; i < taskSum; i++) {
                        final int ii = i;
                        //时间上也要随机,定时不能总是原来越大,也要相应变小
                        long delay = 200 + ii / 100 - (int) (100 * ThreadLocalRandom.current().nextFloat());
                        //使用的简单的方式来分散就可以
                        //long delay = (long)(16 * ThreadLocalRandom.current().nextFloat());
                        //delay = 1;
                        final long date = System.currentTimeMillis() + delay;
                        /**
                         * 添加任务
                         */
                        threadWorker.schedule(() -> {
                            long late = System.currentTimeMillis() - date;
                            if (Math.abs(late) > 50) {
                                misCounter.incrementAndGet();
                            }
                            int count = counter.incrementAndGet();
                            if (count > taskSum) {
                                System.err.println("消费个数大于生产个数");
                                System.exit(1);
                            }

                            /**
                             * bitset虽然不是线程安全的,但是
                             * 1.在这个单线程中访问final
                             * 2.最后通过CountDownLatch保证可见性
                             */
                            assertFalse(bitset.get(ii));
                            bitset.set(ii);

                            if (count == taskSum) {
                                cdl.countDown();
                            }

                        }, delay,TimeUnit.MILLISECONDS);
                    }
                    System.out.println(jj+"入队时间:"+(System.currentTimeMillis()-start)+"/调度误差个数:"+misCounter.get());

                    try {
                        cdl.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    //bitset校验
                    assertEquals(bitset.nextClearBit(0), taskSum);

                    synchronized (allCdl){
                        allCdl[0].countDown();
                    }
                }).start();
            }//~for

            /**
             * 所有线程都执行完了,再执行下一个批次
             */
            try {
                allCdl[0].await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //System.err.print("剩余定时数量:" + threadWorker.getDateSize());

        }//~while
    }

    /**
     * 多线程乱序入队,每一次最后一个任务单独发出
     */
    @Test
    public void testScheduleFromMT2() {
        ScheduledThreadWorker threadWorker = new ScheduledThreadWorker("test1");

        int productThreadSum = 8;
        ExecutorService executor = Executors.newFixedThreadPool(productThreadSum);

        int taskSum = 100000;
        while (true) {
            final AtomicInteger counter = new AtomicInteger(0);
            final AtomicInteger misCounter = new AtomicInteger(0);
            final BitSet bitset = new BitSet(taskSum);
            final CountDownLatch cdl = new CountDownLatch(1);

            long start = System.currentTimeMillis();
            System.out.println("开始入队"+System.currentTimeMillis());

            Function<Integer,Runnable> runnableCreator = (ii)->()->{
                //时间上也要随机,定时不能总是原来越大,也要相应变小
                long delay = 200 + ii / 100 - (int) (100 * ThreadLocalRandom.current().nextFloat());
                delay = 1;
                final long date = System.currentTimeMillis() + delay;
                /**
                 * 添加任务
                 */
                threadWorker.schedule(() -> {
                    long late = System.currentTimeMillis() - date;
                    /**
                     * 调度误差超过50毫秒的就记录下来
                     */
                    if (Math.abs(late) > 50) {
                        misCounter.incrementAndGet();
                    }
                    int count = counter.incrementAndGet();
                    if (count > taskSum) {
                        System.err.println("消费个数大于生产个数");
                        System.exit(1);
                    }

                    /**
                     * bitset虽然不是线程安全的,但是
                     * 1.在这个单线程中访问final
                     * 2.最后通过CountDownLatch保证可见性
                     */
                    assertFalse(bitset.get(ii));
                    bitset.set(ii);

                    if (count == taskSum) {
                        cdl.countDown();
                    }

                }, delay);
            };

            for (int i = 0; i < taskSum-1; i++) {
                executor.execute(runnableCreator.apply(i));
            }//~for

            try {
                Thread.sleep(210);
            } catch (Exception e) {
                e.printStackTrace();
            }

            executor.execute(runnableCreator.apply(taskSum-1));
            //和上面的测试不同,此时入队还没有结束,入队的任务还在executor中
            System.err.println("此时队列中还剩Date:"+threadWorker.getDateSize());

            try {
                cdl.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //bitset校验
            assertEquals(bitset.nextClearBit(0), taskSum);
            System.err.println("剩余定时数量:" + threadWorker.getDateSize()+",超时调度数:"+misCounter.get());
        }
    }

    @Test
    public void testBitSet() {
        int sum = 10000;
        BitSet bitset = new BitSet(sum);
        for (int i = 0; i < sum; i++) {
            bitset.set(i);
        }
        System.err.println(bitset.nextClearBit(0));
    }




    @Test
    public void testPerformance() throws Exception {
        //ExecutorService threadWorker = new ThreadWorker("test1");
        //ExecutorService threadWorker = Executors.newFixedThreadPool(1);
        ExecutorService threadWorker = new ScheduledThreadWorker("test1");

        int producerSum = 10;
        final CountDownLatch cdl = new CountDownLatch(producerSum);

        final int sum = 3000000;
        for (int j = 0; j < producerSum; j++) {
            final int jj = j;
            new Thread(() -> {
                final AtomicInteger counter = new AtomicInteger(0);
                final long start = System.currentTimeMillis();
                for (int i = 0; i < sum; i++) {
                    threadWorker.execute(() -> {
                        int count = counter.incrementAndGet();
                        if (count == sum) {
                            cdl.countDown();
                            System.out.println("第" + jj + "批次共消费" + sum + "个数据,消耗时间" + (System.currentTimeMillis() - start));
                        } else if (count > sum) {
                            System.err.println("消费个数大于生产个数");
                        }
                    });
                }
                System.out.println("第" + jj + "批次入队时间" + (System.currentTimeMillis() - start));
            }).start();
        }

        cdl.await();
        Thread.sleep(1000);

        threadWorker.shutdownNow();
    }


    /**
     * 测试调表
     */
    @Test
    public void skipListTest() throws Exception {
        ConcurrentSkipListMap<Integer, Integer> skipList = new ConcurrentSkipListMap();
        for (int i = 0; i < 1000000; i++) {
            skipList.put(i, i);
        }

        System.out.println("first key:" + skipList.firstKey());
        System.out.println("first key:" + skipList.higherKey(1));
        skipList.remove(2);

        long start = System.currentTimeMillis();
        ArrayList list = new ArrayList();
        for (int i = 0; i < 100000; i++) {
            int h = skipList.higherKey(99999);
            list.add(h);
        }
        System.out.println("查找耗时:" + (System.currentTimeMillis() - start) + "---" + list.size());
    }
}















