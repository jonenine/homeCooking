package mx.homeCooking;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 对线程池的运维任务
 */
abstract class CheckTask {

    static final long minInterval = 60;
    static final long idleInterval = 200;

    final String checkThreadName;
    volatile ThreadWorker checkThread;

    protected CheckTask(ThreadWorker maintainThread) {
        this.checkThread = maintainThread;
        checkThreadName = maintainThread.thread.getName();
        //上来就轮询
        maintainThread.innerSchedule(this::tryCheckAndGetNextDate, minInterval);
    }

    private final AtomicLong nextRunTime = new AtomicLong();


    void changeToCheckThread(){
        if (checkThread == null) {
            checkThread = new ThreadWorker(checkThreadName);
        }
    }

    void changeToWorkers(){
        shutdownCheckThread();
    }

    void shutdownCheckThread(){
        if (checkThread != null) {
            checkThread.shutdown();
            checkThread = null;
        }
    }

    /**
     * 尝试执行并返回下次运行时间
     *
     */
    final long tryCheckAndGetNextDate() {
        long nextDate = nextRunTime.get(), now = System.currentTimeMillis();
        /**
         * 可能会取到一个排他值,说明有其他线程正在执行
         * 这里返回一个轮询最小时间间隔
         */
        if (nextDate == Long.MAX_VALUE) {
            return now + minInterval;
        }

        if (nextDate <= now) {
            /**
             * 排他执行
             */
            if (nextRunTime.compareAndSet(nextDate, Long.MAX_VALUE)) {
                //默认是在维护线程内
                AtomicBoolean isInCheckThread = new AtomicBoolean(true);
                long interval = check(isInCheckThread);
                nextDate = System.currentTimeMillis() + interval;
                nextRunTime.set(nextDate);

                if (isInCheckThread.get()) {
                    changeToCheckThread();
                    //必须已经更新了nextRunTime
                    this.checkThread.innerSchedule(this::tryCheckAndGetNextDate, interval);
                }else{
                    changeToWorkers();
                }
                //返回更新后的时间
                return nextDate;
            }else{
                /**
                 * 没有争抢上,返回一个最小间隔,对获取下次运行更有竞争性
                 */
                return now + minInterval;
            }
        }else{
            /**
             * 没到时间,继续等待
             */
            return nextDate;
        }
    }


    abstract long check(AtomicBoolean returnIsInCheckThread);


}
