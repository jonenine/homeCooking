package mx.homeCooking;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

class ThreadWorker extends AbstractExecutorService {

    final Thread thread;

    public Thread getThread() {
        return thread;
    }

    static final int commonTaskSignal = 1;
    static final int scheduleTaskSignal = 2;
    static final int shutDownSignal = 4;

    final Signal signal = new Signal();

    /**
     * 普通任务队列
     */
    final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue();

    public final Queue<Runnable> getQueue() {
        return queue;
    }

    /**
     * 超时队列中最小的时间
     */
    final AtomicLong minDate = new AtomicLong(Long.MAX_VALUE);
    /**
     * 超时任务队列
     */
    final ConcurrentSkipListMap<Long, ScheduleCommandNode> skipList = new ConcurrentSkipListMap();

    final ConcurrentHashMap<Long, Void> scheduleMapLock = new ConcurrentHashMap(16);

    /**
     * @param name
     */
    ThreadWorker(String name) {
        continueWorking = false;

        thread = new Thread(worker);
        thread.setName(name);
        thread.start();

        //等待线程初始化完毕
        while (!continueWorking) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private volatile boolean continueWorking;

    final Runnable worker = new Runnable() {

        ScheduleCommandNode scheduleTask;

        /**
         * 收集到时的定时任务
         */
        private final long fetchScheduleTasks(long firstDate) {
            final ConcurrentSkipListMap<Long, ScheduleCommandNode> sl = skipList;
            long now = System.currentTimeMillis();
            /**
             * 将所有到点的定时任务都取出,合并在一起
             */
            while (now >= firstDate) {
                ScheduleCommandNode[] turnTask = new ScheduleCommandNode[1];
                //todo:减少下面这个匿名对象可能还能再优化,不过就这样吧
                scheduleMapLock.compute(firstDate, (d, v) -> {
                    //有极小的可能取出来是null
                    turnTask[0] = sl.remove(d);
                    return null;
                });

                if (scheduleTask != null) {
                    scheduleTask.link(turnTask[0]);
                } else if (turnTask[0] != null) {
                    scheduleTask = turnTask[0];
                }
                /**
                 * minDate设置为跳表中更大的那个值,然后可能又会被schedule方法改小
                 * 但下次working循环永远取出最小值
                 */
                Map.Entry<Long, ScheduleCommandNode> firstEntry = sl.firstEntry();
                firstDate = (firstEntry == null) ? Long.MAX_VALUE : firstEntry.getKey();
            }

            return firstDate;
        }

        public final void run() {
            /**
             * starting
             * 将一些对象引用拷贝到方法内部变量,提高访问速度
             */
            final Queue<Runnable> q = queue;
            final AtomicLong md = minDate;
            final AtomicLong consumerC = consumerCount;

            continueWorking = true;

            /**
             * working
             */
            while (continueWorking) {
                /**
                 * 轮询所有超时的定时任务
                 */
                scheduleTask = null;

                md.updateAndGet(this::fetchScheduleTasks);

                if (scheduleTask != null) {
                    scheduleTask.run();
                }

                /**
                 * 如果poll读到offer的写入,task会被顺利执行,然后回到执行循环
                 * 如果poll没有读到offer的写入,会有可能进行入下面的await循环
                 *
                 * 另外类似于linkedBlockingQueue一个方案:
                 * 在poll之前上锁,在出await循环之后解锁
                 * 在await之前置一个队列为空的状态,
                 * 然后在execute先判断状态,再使用condition来进行signal
                 * schedule方法需要将minDate.updateAndGet也写入锁中,然后同样判断state来signal
                 */
                Runnable task = q.poll();

                if (task != null) {
                    try {
                        task.run();
                    } catch (Throwable e) {
                        e.printStackTrace();
                    } finally {
                        consumerC.incrementAndGet();
                    }
                } else if (scheduleTask == null) {
                    /**
                     * 第一次进入await循环立刻就会被唤醒,第二次才会真的await
                     * 如果既没有普通任务,也没有定时任务,就会进入下面的await循环
                     * 下面这个await循环可能会被提前唤醒,提前唤醒甚至离开await循环,回到上面的run循环,都不会影响功能的正常性
                     * (但是会影响性能,但进入await循环都在吞吐量较低的情况下,谁又会在乎此时的性能呢)
                     * 主要防止的是进入await循环后不被唤醒的情况
                     * (1)offer发生在poll之后,signal却不起作用
                     * (2)下面的waitTime没有获取到刚加入的最小date,signal却没起作用
                     */
                    for (; ; ) {
                        /**
                         * 取距离当前最小date需要await的值
                         * 如果没有调度任务,下面让线程await一个很长时间
                         */
                        long waitTime = md.get() - System.currentTimeMillis();

                        if (waitTime > 0) {
                            try {
                                int state;
                                /**
                                 * 等待无果,说明调度任务到点了
                                 * await之后会清除state
                                 * await和set互斥,也就是await循环中不会错过任何一个set
                                 */
                                if ((state = signal.takeState(waitTime, TimeUnit.MILLISECONDS)) == 0) {
                                    break;
                                }

                                if (signal.is(state, shutDownSignal)) {
                                    /**
                                     * 退出
                                     */
                                    break;
                                } else if (signal.is(state, commonTaskSignal)) {
                                    /**
                                     * 有普通任务添加过来了,注意要先判断1,再判断2
                                     * 普通任务相当于延迟是0的调度,优先级更高
                                     */
                                    break;
                                } else if (signal.is(state, scheduleTaskSignal)) {
                                    /**
                                     * 这个最后判断
                                     * 说明有更小的调度来了,重新计算waitTime,再次await
                                     */
                                }
                            } catch (InterruptedException e) {
                                //被中断也会回到上面执行循环
                                break;
                            }
                        } else {
                            /**
                             * 有一个很近的定时任务到点了,回到执行循环
                             */
                            break;
                        }
                    }
                }//~else if
            }//~while

            /**
             * shutdown
             */
            List<Runnable> terminatedTask = (terminatedTasks != null) ? new ArrayList<>() : null;
            for (; ; ) {
                Runnable task = q.poll();
                if (task != null) {
                    if (terminatedTask != null) {
                        terminatedTask.add(task);
                    } else {
                        /**
                         * shutdown后将剩余任务处理完毕,只处理非定时任务
                         */
                        try {
                            task.run();
                        } catch (Throwable e) {
                            e.printStackTrace();
                        } finally {
                            consumerC.incrementAndGet();
                        }
                    }
                } else {
                    break;
                }
            }

            if (terminatedTasks != null) {
                terminatedTasks.offer(terminatedTask);
            }

            /**
             * terminate
             */
            terminationCDL.countDown();
        }//~run
    };

    /**
     * 每一毫秒处理300万,要100年才能耗尽
     */
    private final AtomicLong consumerCount = new AtomicLong(0);

    /**
     * 线程池中缓存的,还没有完成的任务数.传统线程池的size表示的是队列中的任务数
     * 提供快速取size
     */
    public final int getQueueSize() {
        /**
         * 取size的时候再计算最终值
         * 防止生产和消费互相锁定
         */
        int size = (int) (signal.executeCount - consumerCount.get());

        return size < 0 ? 0 : size;
    }

    /**
     * 从这个对列中偷任务
     */
    List<Runnable> stealTask(int stealSize) {
        return signal.stealTask(this.queue, stealSize);
    }

    /**
     * 判断是否空闲
     */
    public final boolean isIdle() {
        return getQueueSize() == 0 && thread.getState() == Thread.State.TIMED_WAITING;
    }


    public long getCompletedTaskCount() {
        return consumerCount.get();
    }


    public final void execute(Runnable command) {
        if (!continueWorking) {
            throw new RejectedExecutionException();
        }

        /**
         * 在shutdown之后再execute可能会丢失这个任务
         * 即
         * (1)上面判定没有结束
         * (2)shutdown
         * (3)worker线程结束
         * (4)再运行下面的代码
         * 这个问题,由group来解决
         */
        if (!queue.offer(command)) {
            throw new RejectedExecutionException();
        }

        /**
         * 这里因为线程调度的原因,极少可能出现上面的offer进去的command都执行完了,才执行的下面的signal.set
         * 此时会将await循环提前唤醒
         */
        signal.setAndAddExecuteCount(commonTaskSignal,1);
    }

    final void execute(List<Runnable> commands) {
        if (!continueWorking) {
            throw new RejectedExecutionException();
        }

        if(commands.isEmpty()) return;

        if (!queue.addAll(commands)) {
            throw new RejectedExecutionException();
        }

        /**
         * 这里因为线程调度的原因,极少可能出现上面的offer进去的command都执行完了,才执行的下面的signal.set
         * 此时会将await循环提前唤醒
         */
        signal.setAndAddExecuteCount(commonTaskSignal, commands.size());
    }


    private class CheckTimeoutRunnable implements Runnable {
        final Runnable command;

        CheckTimeoutRunnable(Runnable command) {
            this.command = command;
        }

        boolean hold = false;

        @Override
        public void run() {
            try {
                command.run();
            } catch (Throwable e) {
                e.printStackTrace();
            }

            synchronized (this) {
                if (!hold) {
                    hold = true;
                } else {
                    Thread.interrupted();
                }
            }
        }

        /**
         * 对check线程的消耗很低
         */
        public final synchronized void checkTimeout() {
            if (!hold) {
                hold = true;
                thread.interrupt();
            }
        }
    }

    /**
     * 超过超时时间就interrupt
     * 还可以使用scheduleWithIn方法来代替这个方法并获得很好的业务性
     *
     * @param command
     * @param timeoutInMillions
     * @param checkThread       一定是线程池之外的一个空闲线程,防止线程池中所有的线程都卡在任务上
     * @return
     */
    public final void executeTimeout(Runnable command, long timeoutInMillions, ThreadWorker checkThread) {
        CheckTimeoutRunnable checkTimeoutRunnable = new CheckTimeoutRunnable(command);
        execute(checkTimeoutRunnable::run);
        checkThread.innerSchedule(checkTimeoutRunnable::checkTimeout, timeoutInMillions);
    }


    /**
     * 不支持毫秒级以下的调度
     */
    public final void innerSchedule(Runnable command, long delay, TimeUnit unit) {
        innerSchedule(command, unit.toMillis(delay));
    }


    /**
     * 调度接口的基础方法
     * 同样的,在shutdown之后再schedule可能会丢失这个任务
     *
     * @param command
     * @param delayInMillions
     */
    public final void innerSchedule(Runnable command, long delayInMillions) {
        if (!continueWorking) {
            throw new RejectedExecutionException();
        }

        if (delayInMillions <= 0) {
            execute(command);
        } else {
            ScheduleCommandNode newNode = new ScheduleCommandNode(command);
            long thisDate = System.currentTimeMillis() + delayInMillions;

            /**
             * 在任务分散时,已经明显好于ScheduledThreadPoolExecutor
             * 在集中状态下,仍然较慢,不过在正常业务情况下,延时是随机的,不可能出现集中情况
             * 或者在延时时加入随机数,以分散之
             */
            scheduleMapLock.compute(thisDate, (d, v) -> {
                /**
                 * skipList有重复问题,现在以链表解决
                 * {@link ConcurrentSkipListMap#compute} JDK的注释:The function is NOT guaranteed to be applied once atomically.
                 * https://stackoverflow.com/questions/53310936/is-concurrentskiplistmap-compute-safe-for-relative-updates
                 * 文章好像是说,对于skipList的值修改而言,是同步的 x=x+1在多线程下是安全的
                 * 但对于回调中的代码而言,并不保证在一个key上串行化(对比concurrentHashMap#comput)
                 * Stephen C(不知何方神圣):好像是因为使用了类似于原子值的自旋算法,从而可能会重复执行回调,而且不对回调做同步
                 *
                 * 因为skipList的这个不严格的同步特性,最后根据测试的结果用ConcurrentHashMap的compute方法对同一个key做同步
                 *
                 * skipList插入时是从小到大的顺序,从实际业务角度讲新的定时任务大都是最大值,所以skipList的算法效率低
                 * 可以将时间变成负值插入.这样就成了找最大值,但找最大值是从head开始找,所以里外里是一样的
                 * 还是入队列的时候慢一点,执行的时候快一点吧(考虑到还有execute的普通任务)
                 */
                skipList.compute(thisDate, (date, node) -> {
                    if (node == null) {
                        node = newNode;
                        //作为头结点而存在
                        node.initHead();
                    } else {
                        node.link(newNode);
                    }
                    return node;
                });

                return null;
            });

            /**
             * 保证每次都成功设置最小值
             */
            if (thisDate == minDate.updateAndGet((oldDate) -> thisDate < oldDate ? thisDate : oldDate)) {
                /**
                 * 更新成功就尝试唤醒
                 * 和execute同样的原因会将await循环提前唤醒
                 */
                signal.set(scheduleTaskSignal);
            }
        }
    }


    static final class ScheduleCommandNode implements Runnable {
        final Runnable task;
        ScheduleCommandNode next;

        ScheduleCommandNode(Runnable task) {
            this.task = task;
        }

        ScheduleCommandNode tail;

        /**
         * 只有head有tail属性
         */
        void initHead() {
            tail = this;
        }

        ScheduleCommandNode link(ScheduleCommandNode nextNode) {
            if (nextNode == null) return this;
            tail.next = nextNode;
            ScheduleCommandNode lastTail = tail;
            //指到nextNode的tail上去,但如果nextNode没有link过,就指到nextNode上
            tail = nextNode.tail;
            if (tail == null) {
                tail = nextNode;
            }
            return lastTail;
        }


        @Override
        public void run() {
            ScheduleCommandNode t = this;
            do {
                try {
                    t.task.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                t = t.next;
            } while (t != null);
        }
    }


    /**
     * date(时间约定)数量,一个时间点上可能有多个task
     * 这个方法有一定成本,只应该用来调试
     */
    public final int getDateSize() {
        return skipList.size();
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            if (continueWorking == true) {
                continueWorking = false;
            } else {
                return;
            }
        }
        /**
         * worker只在一个地方take(clear)
         * 意味着只要写入了shutDownSignal,就一定会在空闲时signal,worker中业务会进入第一个while中,而此时continueWorking已经为false
         * 肯定可以退出循环
         */
        signal.set(shutDownSignal);
    }

    private volatile BlockingQueue<List<Runnable>> terminatedTasks = null;

    List<Runnable> shutdownNow(boolean waitForTerminate) {
        synchronized (this) {
            if (continueWorking == true) {
                //设置terminatedTasks一定要在设置continueWorking前面
                terminatedTasks = new ArrayBlockingQueue<>(1);
                continueWorking = false;
            } else {
                return null;
            }
        }

        signal.set(shutDownSignal);

        if (waitForTerminate) return getTerminatedTask();
        return null;
    }

    List<Runnable> getTerminatedTask() {
        try {
            /**
             * 等待结束
             */
            return terminatedTasks.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return Collections.emptyList();
    }


    @Override
    public List<Runnable> shutdownNow() {
        return shutdownNow(true);
    }

    @Override
    public boolean isShutdown() {
        return !continueWorking;
    }

    private final CountDownLatch terminationCDL = new CountDownLatch(1);

    @Override
    public boolean isTerminated() {
        return terminationCDL.getCount() == 0;
    }

    /**
     * 如果不shutdown就调用这个方法是无意义的,会一直阻塞到超时
     */
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return terminationCDL.await(timeout, unit);
    }

    /**
     * (1)consumer await
     * (2)producer produce
     * (3)producer signal
     * (4)consumer continue
     * <p>
     * 2和1无法同步
     * 所以只能通过保证下面的顺序
     * (1)先1再3
     * (2)先2再3
     * 来同步
     * <p>
     * signal和await机制,需要先await再signal
     * 这里await依赖内部状态state,所以也可以先signal再await
     * <p>
     * 注意还是要先produce再signal
     */
    private final static class Signal {
        final ReentrantLock lock;
        final Condition notZero;

        Signal() {
            lock = new ReentrantLock();
            notZero = lock.newCondition();
        }

        volatile int state = 0;

        /**
         * 内部使用,注意flag的使用
         * set和await方法严格互斥
         */
        void set(int flag) {
            final ReentrantLock l = lock;
            l.lock();
            int _state = state;
            try {
                if (!is(_state, flag)) {
                    state = _state | flag;
                    notZero.signal();
                }
            } finally {
                l.unlock();
            }
        }

        volatile long executeCount;


        void setAndAddExecuteCount(int flag, int batchSize) {
            final ReentrantLock l = lock;
            l.lock();
            int _state = state;
            try {
                if (!is(_state, flag)) {
                    state = _state | flag;
                    notZero.signal();
                }
                executeCount = executeCount + batchSize;
            } finally {
                l.unlock();
            }
        }

        List<Runnable> stealTask(Queue<Runnable> queue, int stealSize) {
            ArrayList<Runnable> steals = new ArrayList<>();
            for (; ; ) {
                Runnable task = queue.poll();
                if (task != null) {
                    steals.add(task);
                    if (steals.size() == stealSize) {
                        break;
                    }
                } else {
                    break;
                }
            }

            if (!steals.isEmpty()) {
                final ReentrantLock l = lock;
                l.lock();
                try {
                    executeCount = executeCount - steals.size();
                } finally {
                    l.unlock();
                }
            }
            return steals;
        }


        /**
         * await之后会清除state,准备好下次的set
         */
        int takeState(long timeout, TimeUnit unit) throws InterruptedException {
            final ReentrantLock l = lock;
            l.lock();
            int _state = state;
            try {
                if (_state != 0 || notZero.await(timeout, unit)) {
                    int res = _state;
                    state = 0;
                    return res;
                } else {
                    return 0;
                }
            } finally {
                l.unlock();
            }
        }


        static boolean is(int state, int flag) {
            return (state & flag) > 0;
        }

    }


}
