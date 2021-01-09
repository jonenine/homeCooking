package mx.homeCooking;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 实现jdk的调度线程池接口
 * 可以通过向schedule方法输入为0的参数delay实现execute promise
 */
public class ScheduledThreadWorker extends ThreadWorker implements ScheduledExecutorService {

    public ScheduledThreadWorker(String name) {
        super(name);
    }

    private final TaskFuture toSchedule(Object command, boolean isCallable, long delayInMillionSeconds) {
        TaskFuture future = isCallable ?
                new TaskFuture(thread, (Callable) command, delayInMillionSeconds) :
                new TaskFuture(thread, (Runnable) command, delayInMillionSeconds);
        this.innerSchedule(future, delayInMillionSeconds);

        return future;
    }

    @Override
    public TaskFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return toSchedule(command, false, unit.toMillis(delay));
    }

    @Override
    public <V> TaskFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return toSchedule(callable, true, unit.toMillis(delay));
    }

    private final void addCancelWhenTimeout(long timeout, ThreadWorker checkThread, TimeUnit unit, TaskFuture<?>[] futures) {
        checkThread.innerSchedule(() -> {
            //防止极端情况
            TaskFuture<?> future0;
            do {
                synchronized (futures) {
                    future0 = futures[0];
                }
            } while (future0 == null);
            future0.cancel(true);
        }, timeout, unit);
    }

    final TaskFuture<?> toScheduleTimeout(Object command, boolean isCallable, long timeout, ThreadWorker checkThread, long delay, TimeUnit unit) {
        if (checkThread == this) {
            throw new RuntimeException("cancel thread can't be self thread");
        }

        final TaskFuture<?>[] futures = new TaskFuture<?>[1];

        TaskFuture<?> future;
        if (isCallable) {
            future = schedule(() -> {
                //启动之后,添加cancel逻辑
                addCancelWhenTimeout(timeout, checkThread, unit, futures);
                //运行原command
                return ((Callable) command).call();
            }, delay, unit);
        } else {
            future = schedule(() -> {
                addCancelWhenTimeout(timeout, checkThread, unit, futures);
                ((Runnable) command).run();
            }, delay, unit);
        }

        synchronized (futures) {
            futures[0] = future;
        }

        return future;
    }


    public TaskFuture<?> scheduleTimeout(Runnable command, long timeout, ThreadWorker checkThread, long delay, TimeUnit unit) {
        return toScheduleTimeout(command, false, timeout, checkThread, delay, unit);
    }


    public <V> TaskFuture<V> scheduleTimeout(Callable<V> callable, long timeout, ThreadWorker checkThread, long delay, TimeUnit unit) {
        return (TaskFuture<V>) toScheduleTimeout(callable, true, timeout, checkThread, delay, unit);
    }


    private final ScheduledFuture<?> toScheduleManyTimes(Runnable command, long initialDelay, long period, TimeUnit unit, boolean isFixedRate) {
        final long commonDelay = unit.toMillis(period);

        /**
         *  每次调度后,都会设置下次调度的future
         */
        final AtomicReference<ScheduledFuture> nextFuture = new AtomicReference<>();

        /**
         * 如已经开始执行下次调度,nextFuture.cancel是没有意义的,此时需要canceled flag起作用
         */
        final AtomicBoolean canceled = new AtomicBoolean(false);

        Runnable task = isFixedRate ? new Runnable() {
            volatile long lastStartTime = System.currentTimeMillis();

            @Override
            public void run() {
                if (canceled.get()) return;

                try {
                    command.run();
                } catch (Throwable e) {
                    e.printStackTrace();
                }


                /**
                 *  计算下次的时间点
                 */
                //ceil操作的要求就是double
                Double timePassed = new Double(System.currentTimeMillis() - lastStartTime);
                //相对当前时间的下一个执行时间点
                long delayToNext = new Double(Math.ceil(timePassed / commonDelay)).longValue() * commonDelay;
                /**
                 * 后续调用
                 */
                if (!canceled.get()) {
                    nextFuture.set(schedule(this, delayToNext, TimeUnit.MILLISECONDS));
                }
            }
        } : new Runnable() {
            @Override
            public void run() {
                if (canceled.get()) return;

                try {
                    command.run();
                } catch (Throwable e) {
                    e.printStackTrace();
                }

                /**
                 * 后续调用
                 */
                if (!canceled.get()) {
                    nextFuture.set(schedule(this, commonDelay, TimeUnit.MILLISECONDS));
                }
            }
        };

        /**
         * 第一次调用
         */
        nextFuture.set(schedule(task, initialDelay, unit));

        /**
         * 继承AbstractFuture为了图省事,除覆盖方法外的其他方法都无意义
         */
        AbstractFuture future = new AbstractFuture(0) {
            @Override
            public long getDelay(TimeUnit unit) {
                /**
                 * 永远返回距离下次调度还有多长时间
                 */
                long delay = nextFuture.get().getDelay(unit);
                //可能正在执行中,还没有生成下一次的future
                return delay < 0 ? 0 : delay;
            }

            /**
             *
             * @param mayInterruptIfRunning 无意义
             * @return
             */
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                canceled.set(true);
                nextFuture.get().cancel(false);

                /**
                 * 终止不了第一次调用,还终止不了第二次吗?所以直接返回true
                 */
                return true;
            }

            @Override
            public boolean isCancelled() {
                return canceled.get();
            }
        };

        return future;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return toScheduleManyTimes(command, initialDelay, period, unit, true);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return toScheduleManyTimes(command, initialDelay, delay, unit, false);
    }

    /**
     * 将所有的定时任务取消
     */
    @Override
    void onShutdown(){
        pollAllScheduledTasks((task) -> {
            if (task instanceof TaskFuture) {
                ((TaskFuture) task).cancel(false);
            }
        });
    }

}
