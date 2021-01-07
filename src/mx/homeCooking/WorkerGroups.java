package mx.homeCooking;

import java.util.concurrent.AbstractExecutorService;

public class WorkerGroups {

    /**
     * 普通线程池
     */
    public static AbstractExecutorService executor(String groupName, int coreSize) {
        return new WorkerGroup(groupName, coreSize, false, false);
    }

    /**
     * 增强型调度池
     */
    public static ScheduledWorkerGroup ScheduledExecutor(String groupName, int coreSize) {
        return new ScheduledWorkerGroup(groupName, coreSize);
    }


}
