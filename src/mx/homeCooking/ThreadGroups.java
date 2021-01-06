package mx.homeCooking;

import java.util.concurrent.AbstractExecutorService;

public class ThreadGroups {

    /**
     * 普通线程池
     */
    public static AbstractExecutorService executor(String groupName, int coreSize) {
        return new ThreadGroup(groupName, coreSize, false, false);
    }

    /**
     * 增强型调度池
     */
    public static ScheduledThreadGroup ScheduledExecutor(String groupName, int coreSize) {
        return new ScheduledThreadGroup(groupName, coreSize);
    }


}
