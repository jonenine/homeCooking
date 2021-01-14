package mx.homeCooking.workGroup;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 实现ScheduledFuture接口
 * 扩展了getCompletionStage方法,实现promise编程方式
 */
public final class TaskFuture<V> extends AbstractFuture<V> implements Runnable {

    final Thread runningThread;
    final Runnable wrappedTask;

    TaskFuture(Thread thread, Runnable task, long delayInMILLISECONDS) {
        super(System.currentTimeMillis() + delayInMILLISECONDS);
        this.runningThread = thread;
        this.wrappedTask = wrapTask(task, false);
    }

    TaskFuture(Thread thread, Callable<V> task, long delayInMILLISECONDS) {
        super(System.currentTimeMillis() + delayInMILLISECONDS);
        this.runningThread = thread;
        this.wrappedTask = wrapTask(task, true);
    }

    TaskFuture(Thread thread) {
        super(0);
        this.runningThread = thread;
        wrappedTask = null;
    }

    /**
     * 手动超时和cancel都用这个exception
     */
    public static final Exception cancelException = new CancellationException("task already canceled or overtime");

    /**
     * 返回promise对象,方便更加灵活的进行链式操作
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
                        synchronized (TaskFuture.this) {
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

    private final boolean complete(V res){
        synchronized (this) {
            Thread.interrupted();
            innerDone = true;
        }
        return result.complete(res);
    }

    private final boolean completeExceptionally(Throwable throwable){
        synchronized (this) {
            Thread.interrupted();
            innerDone = true;
        }
        return result.completeExceptionally(throwable);
    }

    private  boolean innerDone = false;

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
                if (!innerDone && !interruptByCancel) {
                    interruptByCancel = true;
                    /**
                     * 必须和running thread在互斥的块中,否则会将running thread的后续操作interrupt了
                     */
                    runningThread.interrupt();
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 比较精确的表达是否被cancel方法取消掉
     * @return
     */
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

    @Override
    public void run() {
        wrappedTask.run();
    }
}
