package mx.homeCooking;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 实现jdk的调度线程池接口,目前已经显得有些过时
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

        protected final BlockingQueue<Object[]> resultQueue = new ArrayBlockingQueue<>(1);

        AbstractFuture(long cutoffTime) {
            this.cutoffTime = cutoffTime;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long delayInMILLISECONDS = this.cutoffTime - System.currentTimeMillis();

            if (unit.equals(TimeUnit.MILLISECONDS)) {
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
        public synchronized boolean isDone() {
            return resultQueue.size() > 0;
        }

        @Override
        public synchronized V get() throws InterruptedException, ExecutionException {
            Object[] results = resultQueue.take();
            //导致所有使用resultQueue的方法都要加synchronized
            resultQueue.offer(results);
            if (results[1] != null) {
                throw new ExecutionException((Throwable) results[1]);
            }

            return (V) results[0];
        }

        @Override
        public synchronized V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            Object[] results = resultQueue.poll(timeout, unit);
            if (results != null) {
                resultQueue.offer(results);
            }

            if (results == null) {
                throw new TimeoutException();
            } else if (results[1] != null) {
                throw new ExecutionException((Throwable) results[1]);
            }

            return (V) results[0];
        }
    }

    public static final Exception cancelException = new CancellationException();

    final class TaskWrapper<V> extends AbstractFuture<V> implements Delayed {
        final Runnable task;

        public TaskWrapper(Runnable task, long delayInMILLISECONDS) {
            super(System.currentTimeMillis() + delayInMILLISECONDS);
            this.task = wrapTask(task);
        }

        public TaskWrapper(Callable<V> task, long delayInMILLISECONDS) {
            super(System.currentTimeMillis() + delayInMILLISECONDS);
            this.task = wrapTask(task);
        }

        public TaskWrapper() {
            super(0);
            task = null;
        }

        /**
         * 是否占有控制权
         * 在任务开始前抢占控制权
         */
        final AtomicLong holdFlagBeforeStart = new AtomicLong(-1);

        /**
         * 在结束之前抢占控制权
         */
        boolean holdFlagBeforeEnd = false;


        Runnable wrapTask(Object task) {
            return new Runnable() {
                @Override
                public void run() {
                    try {
                        if (holdFlagBeforeStart.compareAndSet(-1, 0)) {
                            Object result = null;
                            if (task instanceof Runnable) {
                                ((Runnable) task).run();
                            } else {
                                result = ((Callable) task).call();
                            }
                            resultQueue.offer(new Object[]{result, null});
                        } else {
                            resultQueue.offer(new Object[]{null, cancelException});
                        }
                    } catch (Throwable e) {
                        Throwable e1 = e;
                        if (e instanceof InterruptedException) {
                            boolean holdByCancelThread;
                            synchronized (task){
                                holdByCancelThread = holdFlagBeforeEnd;
                            }
                            if(holdByCancelThread){
                                e1 = cancelException;
                            }
                        }
                        resultQueue.offer(new Object[]{null, e1});

                    } finally {
                        synchronized (task) {
                            if (!holdFlagBeforeEnd) {
                                //没人占据holdFlagBeforeEnd
                                holdFlagBeforeEnd = true;
                            } else {
                                //holdFlagBeforeEnd已经被占据,当前线程可能已经interrupted
                                Thread.interrupted();
                            }
                        }
                    }
                }
            };
        }

        /**
         * 感觉这个方法仅能表达,这次操作是否有可能将任务cancel
         *
         * @param mayInterruptIfRunning
         * @return 当返回false时, 表示已经执行完了, 或者不做interrupt
         */
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (holdFlagBeforeStart.compareAndSet(-1, thread.getId())) {
                //这是肯定的,不是有可能
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

            boolean holdBeforeEnd;

            /**
             * 确保在task结束前进行thread.interrupt
             */
            synchronized (task) {
                holdBeforeEnd = !holdFlagBeforeEnd;
                if (holdBeforeEnd) {
                    holdFlagBeforeEnd = true;
                    if (mayInterruptIfRunning) thread.interrupt();
                }
            }
            //这只能表达有可能
            return holdBeforeEnd && mayInterruptIfRunning;
        }

        /**
         * 不像cancel,而像isDone方法,会精确地返回是否真的被取消了
         */
        @Override
        public synchronized boolean isCancelled() {
            Object[] result = resultQueue.peek();
            if (result != null) {
                Throwable exp = (Throwable) result[1];
                if (exp == cancelException) {
                    return true;
                }
            }
            return false;
        }
    }


    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return null;
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return null;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return null;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return null;
    }
}
