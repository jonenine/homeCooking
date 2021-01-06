package mx.homeCooking;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ScheduledThreadGroup extends ThreadGroup implements TimeoutScheduledExecutorService {

    ScheduledThreadGroup(String groupName, int coreSize) {
        super(groupName, coreSize, false, true);
    }

    @Override
    public TaskFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return randomProxy().worker.schedule(command, delay, unit);
    }

    @Override
    public <V> TaskFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return randomProxy().worker.schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return randomProxy().worker.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return randomProxy().worker.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override
    public void executeTimeout(Runnable command, long timeoutInMillions) {
        randomProxy().worker.executeTimeout(command, timeoutInMillions, maintainThread);
    }

    @Override
    public TaskFuture<?> scheduleTimeout(Runnable command, long timeout, long delay, TimeUnit unit) {
        return randomProxy().worker.scheduleTimeout(command, timeout, maintainThread, delay, unit);
    }

    @Override
    public <V> TaskFuture<V> scheduleTimeout(Callable<V> callable, long timeout, long delay, TimeUnit unit) {
        return randomProxy().worker.scheduleTimeout(callable, timeout, maintainThread, delay, unit);
    }
}
