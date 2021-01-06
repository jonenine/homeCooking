package mx.homeCooking;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 一.做java传统意义上的线程池时
 * 降低io池添加任务时的同步成本
 * 从初步评测的结果来看,粘滞池的性能>forkjoinpool>恒定bind池>=传统线程池,恒定bind池因为采用排序队列,对cpu性能消耗较大
 * 虽然对io操作的影响不大,主要更快的释放入io池队列的那个线程,也就是io操作的processor的上一个processor的线程,一般是forkJoinPool中的线程
 * 这对于提高吞吐量作用可能还很大
 * <p>
 * 测试时,发现当ThreadGroup接近cpu核心数的两倍的时候,同步的成本降到最低
 * 也就是等于cpu核心数的时候,性能反而不是最好,也就是这个时候还是会撞锁
 * 当io线程池用的时候线程数还是要维持一个比较大数字,比如64,128等
 * <p>
 * 二.做workGroup时
 * 对 execute(String key, Runnable task)方法而言
 * 1.恒定bind方式,一个key只能在某个线程上消费,不会有并发情况,不考虑线程安全问题,这个线程不能回收
 * (1)这种消费方式主要用来实现有状态processor,绑定在一个key上的processor和(同样绑定在这个key上的)其他资源是可以任意互相访问的
 * (2)状态机的问题,因为不用考虑线程同步也简化了许多
 * (3)这种模式下,每个线程都还具有调度功能,为实现超时判定,延迟处理等业务提供了高性能(超过BaseScheduler)和简单(代码不考虑同步)的方案
 * 在对业务模式的灵活性和适应性上,比akka actor又上了一个台阶
 * <p>
 * 2.粘滞性,即key一旦绑定到某个线程上,只要(1)这个线程还在(2)没有新线程加入,就一直绑定上去,如果上条件不满足,就会切换线程
 * 粘滞性是考虑处理线程可能下线的情形,这里对应的是线程收缩的场景,对于某些场景比如数据入库,高峰的时候可能10个线程入库,低谷的时候,入库线程数很少(0或1)
 * 这样做的优点是,同一个时刻,对一个key而言,尽量只有一个线程来调用task,降低并发成本.但仍然会出现线程切换和并发的情况,有状态的processor中还是要做好同步
 * <p>
 * 粘滞性模式的方案(比如入库)不如绑定线程+无状态processor的方案简洁明了,不过性能上就不好说了
 */
public class ThreadGroup extends AbstractExecutorService {

    final ThreadProxy[] proxies;
    final int maxAmount;
    /**
     * 每次增加的线程数
     */
    final int incrementNum;
    final String groupName;

    final boolean isBind;
    final boolean isSchedule;


    /**
     * 当前线程数
     */
    volatile int amount = 0;

    /**
     * @param groupName
     * @param coreSize
     * @param isBind      是否是绑定池,绑定池是调度的,而且是Timeout池
     * @param isSchedule  是否提供调度线程池的功能
     */
    public ThreadGroup(String groupName, int coreSize, boolean isBind, boolean isSchedule) {
        this.groupName = groupName;
        maxAmount = coreSize;
        incrementNum = coreSize >= 100 ? 3 : (coreSize >= 30 ? 2 : 1);
        this.isBind = isBind;
        this.isSchedule = isSchedule;
        proxies = new ThreadProxy[coreSize];

        for (int i = 0; i < coreSize; i++) {
            proxies[i] = new ThreadProxy(i);
            if (isBind || isSchedule) {
                proxies[i].start(null);
            }
        }
    }


    public void execute(int hash, Runnable task) {
        if (this.isBind) {
            /**
             * 幂等性模式下,实际是绑定线程消费,即对某个key而言,其对应的线程是不变的
             * 有状态processor可以不用考虑同步问题
             */
            ThreadProxy proxy = proxies[hash % maxAmount];
            proxy.execute(task);
        } else {
            ThreadProxy proxy;
            do {
                int _mount = amount;
                if (_mount == 0) {
                    throw new RejectedExecutionException();
                }
                /**
                 * 1.amount一旦改变,就会切换到其他线程
                 * 2.所以,这样做仍然会出现对一个key的操作出现并发的情况,只是大大减少这种情况(同时降低同步成本)
                 * 有状态processor在这个模式下还是需要考虑线程安全问题
                 */
                proxy = proxies[hash % _mount];
            } while (!proxy.execute(task));
        }
    }


    /**
     * 此方法看来已经保证在同时发生shutdown时不会丢失任务
     */
    @Override
    public void execute(Runnable task) {
        int hash = (int) Thread.currentThread().getId();
        int _mount = amount;
        while (_mount > 0) {
            /**
             * 如果proxy已经stop了,就再次分配
             */
            if (proxies[hash % _mount].execute(task)) {
                return;
            }
            _mount--;
        }

        proxies[0].start(task);
    }

    final CheckRunnable checkRunnable = new CheckRunnable() {

        volatile long startLazyTime = 0;
        volatile int queueSizeSum = 0;

        long check() {
            //空闲的ThreadProxy
            List<ThreadProxy> idleList = new ArrayList<>();
            //不空闲的ThreadProxy
            List<ThreadProxy> busyList = new ArrayList<>();

            queueSizeSum = 0;

            for (int i = 0; i < maxAmount; i++) {
                ThreadProxy proxy = proxies[i];
                int queueSize = proxy.tempQueueSize = proxy.queueSize(null);
                if (queueSize == 0) {
                    idleList.add(proxy);
                } else {
                    busyList.add(proxy);
                }

                queueSizeSum += queueSize;
            }

            int _amount = amount;
            /**
             * 只要队列中的任务数比当前线程数大,就增加新线程,反之懒惰的减少线程
             * 基本上,就是一下子就会压到最大值
             */
            if (queueSizeSum > _amount) {
                if (_amount < maxAmount) {
                    //每次最多新增incrementNum个新线程
                    for (int i = 0; i < incrementNum; i++) {
                        //下面的start会改变amount,所以这里重读
                        _amount = amount;
                        if (_amount < maxAmount) {
                            ThreadProxy newProxy = proxies[_amount];
                            //重复start不影响业务正确性
                            newProxy.start();
                        } else {
                            break;
                        }
                    }
                }

                //清空空闲状态
                startLazyTime = 0;
            } else {
                //线程收缩主要在粘滞池的情况,采用普通线程,没有延迟任务
                if (_amount > 0) {
                    long currentTime = System.currentTimeMillis();
                    if (startLazyTime == 0) {
                        //设置空闲状态
                        startLazyTime = currentTime;
                    } else {
                        //空闲时,每隔一段时间消减一个线程
                        if (currentTime - startLazyTime > 2000) {
                            proxies[_amount - 1].stop(false);
                            //清空空闲状态
                            startLazyTime = 0;
                        }
                    }
                }
            }

            /**
             * 因为后添加的线程都是右边线程,所以左边的旧线程积累的任务通常较多
             * 所以在线程数达到最大之后,而且当某个线程队列一直为0的时候(意味着入队已经停止一段时间),开始重平衡的操作
             */
            if (amount == maxAmount && !idleList.isEmpty()) {
                //按照上面取出的队列长度从大到小排序
                Collections.sort(busyList, new Comparator<ThreadProxy>() {
                    @Override
                    public int compare(ThreadProxy o1, ThreadProxy o2) {
                        return o2.tempQueueSize - o1.tempQueueSize;
                    }
                });
                for (int i = 0, l = busyList.size(); i < l; i++) {
                    //忙的线程匹配闲的线程
                    if (i < idleList.size()) {
                        ThreadProxy idleWorker = idleList.get(i);
                        ThreadProxy busyWorker = busyList.get(i);
                        Runnable rebalanceTask = idleWorker.rebalanceTask(busyWorker.index, busyWorker.tempQueueSize);
                        idleWorker.execute(rebalanceTask);
                    }
                }//~for
            }//~if

            return amount == maxAmount ? 1000 : 60;
        }

        @Override
        long run() {
            /**
             * 和下面的shutdown方法互斥
             */
            synchronized (ThreadGroup.this) {
                if (alreadyShutDown || isBind) {
                    return Long.MAX_VALUE;
                }
                return check();
            }
        }

    };


    volatile boolean alreadyShutDown = false;

    /**
     * 这个方法不阻塞
     * shutdown是一个个进行的,按照proxy start顺序的逆序
     */
    Stream<ScheduledThreadWorker> toShutdown(boolean isShutdownNow) {
        List<ThreadProxy> reverseProxies = Arrays.asList(proxies);
        //注意倒叙操作
        Collections.reverse(reverseProxies);
        terminatingWorker = reverseProxies.stream()
                .map(proxy -> proxy.stop(isShutdownNow))
                .filter(worker -> worker != null);

        return terminatingWorker;

    }

    @Override
    public synchronized void shutdown() {
        if (!alreadyShutDown) {
            alreadyShutDown = true;
            toShutdown(false);
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        boolean isMyTurn = false;
        synchronized (this) {
            if (!alreadyShutDown) {
                alreadyShutDown = true;
                toShutdown(true);
                isMyTurn = true;
            }
        }

        return isMyTurn ? terminatingWorker
                //getTerminatedTask这里会阻塞
                .flatMap(worker -> worker.getTerminatedTask().stream()).collect(Collectors.toList())
                : null;
    }

    volatile Stream<ScheduledThreadWorker> terminatingWorker;

    @Override
    public boolean isShutdown() {
        if (terminatingWorker == null) {
            return false;
        } else {
            return terminatingWorker.allMatch(worker -> worker.isShutdown());
        }
    }

    @Override
    public boolean isTerminated() {
        if (terminatingWorker == null) {
            return false;
        } else {
            return terminatingWorker.allMatch(worker -> worker.isTerminated());
        }
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (terminatingWorker == null) {
            Thread.sleep(unit.toMillis(timeout));
            return isTerminated();
        } else {
            InterruptedException[] es = new InterruptedException[1];
            boolean re = terminatingWorker.allMatch(worker -> {
                try {
                    return worker.awaitTermination(timeout, unit);
                } catch (InterruptedException e) {
                    es[0] = e;
                    return false;
                }
            });

            if (es[0] != null) {
                throw es[0];
            }

            return re;
        }
    }

    final class ThreadProxy {
        final int index;
        private final ReentrantReadWriteLock.WriteLock writeLock;
        private final ReentrantReadWriteLock.ReadLock readLock;

        ThreadProxy(int index) {
            this.index = index;
            ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
            writeLock = lock.writeLock();
            readLock = lock.readLock();
        }

        volatile ScheduledThreadWorker executor;

        void start() {
            ReentrantReadWriteLock.WriteLock lock = writeLock;
            lock.lock();
            try {
                if (executor == null) {
                    /**
                     * 会不会和shutdown流程冲突
                     * 这里和stop互斥,进入这里意味着肯定已经stop(在stop之前进入什么都不做)
                     * 而stop发生在alreadyShutDown标志位之后,也就是说肯定会抛出异常
                     */
                    if (alreadyShutDown) {
                        throw new RejectedExecutionException();
                    }
                    executor = new ScheduledThreadWorker(groupName + "-" + index);
                    amount++;
                }
            } finally {
                lock.unlock();
            }
        }

        void start(Runnable task) {
            /**
             * execute成功返回时会保证在(shutdown中的)stop之前发生
             * 而shutdownNow收集terminating task保证在stop之后发生,也就是会保证所有的任务都不会丢失
             */
            while (!execute(task)) {
                start();
            }
        }

        /**
         *
         */
        ScheduledThreadWorker stop(boolean isShutDownNow) {
            ScheduledThreadWorker _executor;

            ReentrantReadWriteLock.WriteLock lock = writeLock;
            lock.lock();
            try {
                _executor = executor;
                if (_executor != null) {
                    amount--;
                    executor = null;
                }
            } finally {
                lock.unlock();
            }

            if (_executor == null) {
                return null;
            }

            if (isShutDownNow) {
                _executor.shutdownNow(false);
            } else {
                _executor.shutdown();
            }

            return _executor;
        }

        /**
         * 如果内部的线程池已经或正在销毁,就返回false
         */
        boolean execute(Runnable task) {
            ReentrantReadWriteLock.ReadLock lock = readLock;
            lock.lock();
            try {
                ScheduledThreadWorker _executor = executor;
                if (_executor != null) {
                    try {
                        _executor.execute(task);
                        return true;
                    } catch (RejectedExecutionException e) {
                        e.printStackTrace();
                    }
                }

                return false;
            } finally {
                lock.unlock();
            }
        }

        int tempQueueSize;

        Runnable rebalanceTask(int busyIndex, int busyQueueSize) {
            return () -> {
                ThreadProxy busy = proxies[busyIndex];
                //从繁忙线程窃取的任务数
                int stolenSize = busyQueueSize / 2;

                Queue formQueue = busy.getTaskQueue();
                Queue toQueue = getTaskQueue();
                while (--stolenSize > 0) {
                    Runnable task = (Runnable) formQueue.poll();
                    if (task == null) {
                        break;
                    }
                    /**
                     * 如果此任务正在执行时shutdown
                     * ScheduledThreadWorker会等待当前正在执行的任务执行完毕再进入shutdown流程
                     * 所以进入shutdown流程之前,这些task都已经入队,不会被丢弃
                     */
                    toQueue.offer(task);
                }
            };
        }

        int queueSize(ScheduledThreadWorker _executor) {
            _executor = _executor != null ? _executor : executor;
            if (_executor != null) {
                return _executor.size();
            }

            return 0;
        }

        Queue getTaskQueue() {
            final ScheduledThreadWorker _executor = executor;
            if (_executor != null) {
                return _executor.getQueue();
            }

            return null;
        }

        ScheduledThreadWorker getExecutor() {
            return executor;
        }
    }

}























