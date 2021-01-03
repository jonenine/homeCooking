package mx.homeCooking;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 实现jdk的调度线程池接口
 */
public class ScheduledThreadWorker extends ScheduledThreadWorkerBase implements ScheduledExecutorService {

    ScheduledThreadWorker(String name, Runnable check) {
        super(name, check);
    }

    public ScheduledThreadWorker(String name) {
        super(name);
    }


    abstract class AbstractFuture<V> implements ScheduledFuture<V> {

        protected final long cutoffTime;

        /**
         * 占内存应该比一个queue少
         */
        protected final CompletableFuture<V> result = new CompletableFuture();

        protected final synchronized boolean completeExceptionally(Throwable throwable){
            boolean isDone = result.completeExceptionally(throwable);
            Thread.interrupted();
            return isDone;
        }

        protected final synchronized boolean complete(V res){
            boolean isDone =  result.complete(res);
            Thread.interrupted();
            return isDone;
        }

        AbstractFuture(long cutoffTime) {
            this.cutoffTime = cutoffTime;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long delayInMILLISECONDS = this.cutoffTime - System.currentTimeMillis();

            if (unit == null || unit.equals(TimeUnit.MILLISECONDS)) {
                return delayInMILLISECONDS;
            }
            /**
             * DelayQueue里面的实现是纳秒
             */
            if (unit.equals(TimeUnit.NANOSECONDS)) {
                return delayInMILLISECONDS * 1000000;
            }

            if (unit.equals(TimeUnit.MICROSECONDS)) {
                return delayInMILLISECONDS * 1000;
            }

            throw new RuntimeException();
        }

        @Override
        public int compareTo(Delayed o) {
            AbstractFuture that = (AbstractFuture) o;
            /**
             * 从小到大排序
             */
            return (int) (this.cutoffTime - that.cutoffTime);
        }

        @Override
        public boolean isDone() {
            return result.isDone();
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return result.get();
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            V v = result.get(timeout, unit);
            if (v == null) {
                throw new TimeoutException();
            }
            return v;
        }
    }

    public static final Exception cancelException = new CancellationException();

    public final class TaskWrapper<V> extends AbstractFuture<V> implements Runnable {
        final Runnable wrappedTask;

        TaskWrapper(Runnable task, long delayInMILLISECONDS) {
            super(System.currentTimeMillis() + delayInMILLISECONDS);
            this.wrappedTask = wrapTask(task, false);
        }

        TaskWrapper(Callable<V> task, long delayInMILLISECONDS) {
            super(System.currentTimeMillis() + delayInMILLISECONDS);
            this.wrappedTask = wrapTask(task, true);
        }

        TaskWrapper() {
            super(0);
            wrappedTask = null;
        }

        /**
         * 返回promise对象,方便更加灵活的进行链式操作,不过可能会因此卡死cancel方法
         * 所以mayInterruptIfRunning==true不是一个好的选择
         */
        public final CompletionStage<V> getCompletionStage() {
            return result;
        }

        /**
         * 是否占有控制权
         * 在任务开始前抢占控制权
         */
        final AtomicLong holdFlagBeforeStart = new AtomicLong(-1);

        /**
         * 中断是否是cancel方法引起的
         */
        boolean interruptByCancel = false;

        final Runnable wrapTask(Object task, boolean isCallable) {
            return new Runnable() {
                @Override
                public void run() {
                    if (holdFlagBeforeStart.compareAndSet(-1, 0)) {
                        V res = null;
                        try {
                            if (isCallable) {
                                res = (V) ((Callable) task).call();
                            } else {
                                ((Runnable) task).run();
                            }
                            //synchronized(future)
                            complete(res);
                        } catch (InterruptedException e) {
                            Exception e1 = e;
                            boolean byCancel;
                            synchronized (TaskWrapper.this) {
                                byCancel = interruptByCancel;
                            }
                            /**
                             * 如果这个中断是cancel方法引起的,有可能是业务代码自己触发的中断(这样很危险)
                             * 最终以影响的结果是isCancelled方法,这个isCancelled方法意义不是很大
                             */
                            if(byCancel){
                                e1 = cancelException;
                            }
                            completeExceptionally(e1);
                        } catch (Throwable e) {
                            //业务代码中抛出的其他异常
                            completeExceptionally(e);
                        }
                    }
                }//~run
            };
        }

        @Override
        public void run() {
            wrappedTask.run();
        }

        /**
         * 取消一个任务,不会从任务队列中删除,而是不再执行
         * @param mayInterruptIfRunning
         * @return 当返回false时, 表示已经执行完了, 或者mayInterruptIfRunning==false,但返回为true时表示在task开始之前成功取消了
         * 或者...不知道了(看task内部怎么处理了)
         */
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (holdFlagBeforeStart.compareAndSet(-1, 1)) {
                //设置get时抛出的异常,会被包装成executionException
                result.completeExceptionally(cancelException);
                //这里肯定取消了,不是有可能
                return true;
            } else {
                //判断是否被其他cancel线程所占据
                if (holdFlagBeforeStart.get() != 0) {
                    return false;
                }
            }
            /**
             * holdFlagBeforeStart.get()==0意味着running
             */

            if(mayInterruptIfRunning){
                synchronized (this) {
                    if (!result.isDone() && !interruptByCancel) {
                        interruptByCancel = true;
                        thread.interrupt();
                        return true;
                    }
                }
            }

            return false;
        }


        @Override
        public boolean isCancelled() {
            if (result.isCompletedExceptionally()) {
                try {
                    //这里会抛出异常,可能会影响一些性能
                    result.get();
                } catch (InterruptedException e) {

                } catch (ExecutionException e) {
                    if (e.getCause() == cancelException) {
                        return true;
                    }
                } catch (Throwable e) {

                }
            }
            return false;
        }
    }

    private final TaskWrapper toSchedule(Object command, long delayInMillionSeconds) {
        TaskWrapper wrapper = command instanceof Runnable ?
                new TaskWrapper((Runnable) command, delayInMillionSeconds)
                : new TaskWrapper((Callable) command, delayInMillionSeconds);
        this.innerSchedule(wrapper, delayInMillionSeconds);

        return wrapper;
    }


    @Override
    public TaskWrapper<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return toSchedule(command, unit.toMillis(delay));
    }

    @Override
    public <V> TaskWrapper<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return toSchedule(callable, unit.toMillis(delay));
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
         * 除覆盖方法外的其他方法都无意义
         */
        AbstractFuture future = new AbstractFuture(0) {
            @Override
            public long getDelay(TimeUnit unit) {
                /**
                 * 永远返回距离下次调度还有多长时间
                 */
                return nextFuture.get().getDelay(unit);
            }

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
}
