package mx.homeCooking.workGroup;

import mx.homeCooking.collections.ConcurrentBitSet;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class WorkerTest {

    public static void main(String[] args) {
        try {
            WorkerTest tester = new WorkerTest();
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
        ExecutorService threadWorker = new ThreadWorker("test1");

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


    final ThreadLocal<Boolean> shutdownLocal = new ThreadLocal<>();

    /**
     * 测试shutDown,以及shutdown后处理遗留任务
     */
    @Test
    public void testShutDown() throws Exception {
        ThreadWorker threadWorker = new ThreadWorker("test1");

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
                    System.out.println("完成消费" + count + "/" + threadWorker.getQueueSize());
                } else if (count > sum) {
                    System.err.println("消费个数大于生产个数");
                    System.exit(1);
                }
            });
        }

        try {
            Boolean isShutdownNow = shutdownLocal.get();
            if (isShutdownNow == null || !isShutdownNow) {
                threadWorker.shutdown();
            } else {
                List<Runnable> terminatedTasks = threadWorker.shutdownNow();
                System.out.println("shutdownNow后剩余任务数" + terminatedTasks.size());
            }

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
        int remain = threadWorker.getQueueSize();

        System.out.println("当前消费:" + count + "/剩余:" + remain + "/消费+剩余:" + (count + remain));
        while (threadWorker.getThread().isAlive()) {

        }
        System.err.println(threadWorker.getQueueSize());
    }

    @Test
    public void testShutDownNowManyTimes() throws Exception {
        shutdownLocal.set(true);
        for (int i = 0; i < 1000; i++) {
            testShutDown();
        }
    }

    @Test
    public void testShutDownManyTimes() throws Exception {
        for (int i = 0; i < 1000; i++) {
            testShutDown();
        }

    }

    static void sleep(long l) {
        try {
            Thread.sleep(l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testThreadState() throws Exception {
        ThreadWorker threadWorker = new ThreadWorker("test");

        long start = System.currentTimeMillis();
        Thread.State state;
        do {
            sleep(1);
            state = threadWorker.getThread().getState();
            //System.out.println("开始:"+state);
        } while (state != Thread.State.TIMED_WAITING);
        System.out.println("线程创建后需要" + (System.currentTimeMillis() - start) + "才可以进入TIMED_WAITING");


        threadWorker.execute(() -> {
            System.out.print("正在执行任务:");
            System.out.println(threadWorker.getThread().getState());
        });
        sleep(10);


        System.out.println("执行任务之后:" + threadWorker.getThread().getState());
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
                ThreadWorker threadWorker = new ThreadWorker("test" + ii);
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
                            Thread.sleep(threadWorker.getQueueSize() % 3);
                            //System.out.println("-----:" + threadWorker.size());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        //3.发最后一个任务,看能否唤醒
                        new Thread(() -> {
                            threadWorker.execute(() -> {
                                //此时threadWorker肯定是1,因为这个runnable还没有执行完成呢!
                                System.out.println("最后一个任务消费完毕!");
                            });
                        }).start();

                        System.out.println("-----:" + threadWorker.getQueueSize());

                        while (threadWorker.getQueueSize() > 0) {

                        }
                        System.out.println(count.incrementAndGet() + "/" + threadWorker.getQueueSize());
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
        ThreadWorker threadWorker = new ThreadWorker("test1");
        //ScheduledWorkerGroup threadWorker = WorkerGroups.scheduledExecutor("test", 8);
        //ScheduledExecutorService threadWorker = Executors.newScheduledThreadPool(8);


        final AtomicInteger counter = new AtomicInteger(0);
        new Thread(() -> {
            int last = counter.get();
            while (true) {
                try {
                    Thread.sleep(1000);
                    int now = counter.get();
                    System.err.println(new Date()+"---"+now+"----"+(now-last));
                    last = now;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        Random r = new Random();
        int sum = 1000000;

        while (true) {
            counter.set(0);
            final ConcurrentBitSet bitset = new ConcurrentBitSet(sum);

            CountDownLatch cdl = new CountDownLatch(1);
            for (int i = 0; i < sum; i++) {
                final int ii = i;
                //时间上也要随机,定时不能总是原来越大,也要相应变小
                final long delay = 10 + ii / 100 - (int) (10 * r.nextFloat());
                final long date = System.currentTimeMillis() + delay;

                threadWorker.innerSchedule(() -> {
                    long late = System.currentTimeMillis() - date;
                    if (Math.abs(late) > 100) {
                        System.err.println("调度误差超过100毫秒:" + late);
                    }
                    int count = counter.incrementAndGet();
                    if (count > sum) {
                        System.err.println("消费个数大于生产个数");
                        System.exit(1);
                    }

                    assertFalse(bitset.get(ii));
                    bitset.set(ii);

                    /**
                     * 定时调度的结束时间等于最后一个任务被调度而且完成时间,所以多线程调用不一定比单线程快
                     */
                    if (count == sum) {
                        cdl.countDown();
                    }

                }, delay,TimeUnit.MILLISECONDS);
            }

            try {
                cdl.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //引入bitset做一个校验
            assertEquals(bitset.nextClearBit(0), sum);
            System.err.println("-------------------------------------------------");
            //System.err.print("剩余定时数量:" + threadWorker);
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

        //ThreadWorker threadWorker = new ThreadWorker("test1");
        ScheduledThreadPoolExecutor threadWorker = new ScheduledThreadPoolExecutor(8);
        /**
         * randomProxy算法降速严重
         */
        //ScheduledWorkerGroup threadWorker = WorkerGroups.scheduledExecutor("test", 8);

        CountDownLatch[] allCdl = new CountDownLatch[1];
        AtomicInteger[] counters = new AtomicInteger[productThreadSum];

        //flush thread
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    StringBuilder sb = new StringBuilder();
                    for (AtomicInteger counter : counters) {
                        sb.append(counter.get() + ",");
                    }
                    //System.err.println(new Date()+"/"+threadWorker.getDateSize()+"---- "+sb);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        while (true) {
            synchronized (allCdl) {
                allCdl[0] = new CountDownLatch(productThreadSum);
            }
            for (int j = 0; j < productThreadSum; j++) {
                final AtomicInteger counter = counters[j] = new AtomicInteger(0);
                final AtomicInteger misCounter = new AtomicInteger(0);
                final ConcurrentBitSet bitset = new ConcurrentBitSet(taskSum);
                final CountDownLatch cdl = new CountDownLatch(1);

                int jj = j;
                /**
                 * 以多线程入同一个队列,多线程之间彼此不知道
                 */
                new Thread(() -> {
                    long start = System.currentTimeMillis();
                    System.out.println(jj + "开始入队");
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

                        }, delay, TimeUnit.MILLISECONDS);
                    }
                    System.out.println(jj + "入队时间:" + (System.currentTimeMillis() - start) + "/调度误差个数:" + misCounter.get());

                    try {
                        cdl.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    //bitset校验
                    assertEquals(bitset.nextClearBit(0), taskSum);

                    synchronized (allCdl) {
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
        ThreadWorker threadWorker = new ThreadWorker("test1");

        int productThreadSum = 8;
        //ExecutorService executor = Executors.newFixedThreadPool(productThreadSum);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(8);

        int taskSum = 100000;
        while (true) {
            final AtomicInteger counter = new AtomicInteger(0);
            final AtomicInteger misCounter = new AtomicInteger(0);
            final BitSet bitset = new BitSet(taskSum);
            final CountDownLatch cdl = new CountDownLatch(1);

            long start = System.currentTimeMillis();
            System.out.println("开始入队" + System.currentTimeMillis());

            Function<Integer, Runnable> runnableCreator = (ii) -> () -> {
                //时间上也要随机,定时不能总是原来越大,也要相应变小
                long delay = 200 + ii / 100 - (int) (100 * ThreadLocalRandom.current().nextFloat());
                delay = 1;
                final long date = System.currentTimeMillis() + delay;
                /**
                 * 添加任务
                 */
                threadWorker.innerSchedule(() -> {
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

            for (int i = 0; i < taskSum - 1; i++) {
                executor.execute(runnableCreator.apply(i));
            }//~for

            try {
                Thread.sleep(210);
            } catch (Exception e) {
                e.printStackTrace();
            }

            executor.execute(runnableCreator.apply(taskSum - 1));
            //和上面的测试不同,此时入队还没有结束,入队的任务还在executor中
            System.err.println("此时队列中还剩Date:" + threadWorker.getDateSize());

            try {
                cdl.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //bitset校验
            assertEquals(bitset.nextClearBit(0), taskSum);
            System.err.println("剩余定时数量:" + threadWorker.getDateSize() + ",超时调度数:" + misCounter.get());
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
        ExecutorService threadWorker = new ThreadWorker("test1");

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

    @Test
    public void testCondition() throws Exception {
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        for (int i = 0; i < 1000; i++) {
            final int j = i;
            final AtomicInteger counter = new AtomicInteger(0);
            Thread t = new Thread(() -> {
                lock.lock();
                try {
                    counter.set(1);
                    condition.await();
                    counter.set(2);
                    System.err.println(j+" leave");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            });

            t.start();

            while (counter.get() == 0) {
                //wait
            }

            /**
             * 此测试证明是signal所在线程释放锁之后,await线程才能从await方法返回
             * 1.从源码看,是这样的,signal将await线程unpark了
             * 2.从锁的原子性来看也是这样的
             * 3.一个锁定中间插入其他的线程锁定后再恢复到原锁定也没法通过signal实现
             */
            while (counter.get() ==1) {
                lock.lock();
                if(counter.get() ==1){
                    condition.signal();
                    for (int l = 0; l < 10; l++) {
                        if (counter.get() == 2) {
                            System.err.println("----------------------" + counter.get());
                            System.exit(1);
                        }
                    }
                }

                lock.unlock();
            }


            t.join();
        }
    }
}















