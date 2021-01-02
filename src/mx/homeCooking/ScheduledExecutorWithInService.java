package mx.homeCooking;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 增加一些withIn操作
 * 当超过超时时间时,会统一在正在执行的任务线程中触发interrupt
 * 执行任务是否停止在于
 * 1.是否使用interruptable io
 * 2.是否在循环中判断 thread.interrupted
 * 3.以及在try catch之后是否可以跳出循环
 * 否则withIn的设置是无效的
 */
public interface ScheduledExecutorWithInService {

    void executeWithIn(Runnable command, long timeout, TimeUnit unit);

    ScheduledFuture<?> scheduleWithIn(Runnable command, long timeout, long delay, TimeUnit unit);

    <V> ScheduledFuture<V> scheduleWithIn(Callable<V> callable, long timeout, long delay, TimeUnit unit);
}
