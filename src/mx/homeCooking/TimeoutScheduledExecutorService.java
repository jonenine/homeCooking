package mx.homeCooking;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public interface TimeoutScheduledExecutorService extends  ScheduledExecutorService {

    void executeTimeout(Runnable command, long timeoutInMillions);

    TaskFuture<?> scheduleTimeout(Runnable command, long timeout, long delay, TimeUnit unit);

    <V> TaskFuture<V> scheduleTimeout(Callable<V> callable, long timeout, long delay, TimeUnit unit);
}