package mx.homeCooking.workGroup;

import java.util.concurrent.*;

abstract class AbstractFuture<V> implements ScheduledFuture<V> {

    protected final long cutoffTime;

    /**
     * 占内存应该比一个queue少
     */
    protected final CompletableFuture<V> result = new CompletableFuture();


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
