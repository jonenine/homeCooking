package mx.homeCooking.workGroup;

import java.util.concurrent.AbstractExecutorService;

public class WorkerGroups {

    /**
     * 普通线程池,此线程池的线程数量会收缩,当线程池数量达到最大值coreSize的时候
     * 会关闭维护线程,并采用worker中随机线程来调度维护任务,也就是说维护任务可能会被所在线程队列前面的任务卡住
     * 从而造成各个队列任务重平均工作执行频率下降
     */
    public static AbstractExecutorService executor(String groupName, int coreSize) {
        return new WorkerGroup(groupName, coreSize, false, false);
    }

    /**
     * 支持timeout操作的executor,此线程池的线程数量会收缩
     * io操作中有很多需要超时取消的操作
     */
    public static WorkerGroup timeoutExecutor(String groupName, int coreSize) {
        return new WorkerGroup(groupName, coreSize, false, false){
            @Override
            boolean isChangeCheckThread() {
                /**
                 * 不再切换check thread,始终保持maintainThread存在
                 */
                return false;
            }
        };
    }

    /**
     * 增强型调度池,此线程池因为线程数量恒定,锁少一些,所以入队性能要明显好于普通线程池
     * 调度池虽然线程数固定,但仍有维护线程用来重平衡普通任务和进行超时检测的工作
     */
    public static ScheduledWorkerGroup scheduledExecutor(String groupName, int coreSize) {
        return new ScheduledWorkerGroup(groupName, coreSize);
    }


}
