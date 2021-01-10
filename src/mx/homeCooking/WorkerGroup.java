package mx.homeCooking;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
public class WorkerGroup extends AbstractExecutorService {

    protected final ThreadProxy[] proxies;
    private final int maxAmount;
    /**
     * 每次增加的线程数
     */
    private final int incrementNum;
    private final String groupName;

    private final boolean isBind;
    private final boolean isSchedule;
    /**
     * 是否要切换check thread
     */
    private final boolean changeCheckThread;

    boolean isChangeCheckThread() {
        return !isBind && !isSchedule;
    }

    /**
     * 线程数量是否伸缩
     */
    private final boolean shrinkThreadPool;


    /**
     * 当前线程数
     */
    private volatile int amount = 0;

    /**
     * 运维线程池
     */
    protected volatile ThreadWorker maintainThread;


    /**
     * @param groupName
     * @param coreSize
     * @param isBind     是否是绑定池,绑定池是调度的,而且是Timeout池
     * @param isSchedule 是否提供调度线程池的功能,不一定支持timeout池,如果支持timeout需要独立的timeout线程
     */
    WorkerGroup(String groupName, int coreSize, boolean isBind, boolean isSchedule) {
        this.groupName = groupName;
        maxAmount = coreSize;
        incrementNum = coreSize >= 100 ? 3 : (coreSize >= 30 ? 2 : 1);
        this.isBind = isBind;
        this.isSchedule = isSchedule;
        proxies = new ThreadProxy[coreSize];
        this.changeCheckThread = isChangeCheckThread();
        this.shrinkThreadPool = !isBind && !isSchedule;

        for (int i = 0; i < coreSize; i++) {
            proxies[i] = new ThreadProxy(i);
            if (isBind || isSchedule) {
                proxies[i].start();
            }
        }

        /**
         * bind池用来timeout
         * schedule池和普通池用来check和timeout
         */
        maintainThread = new ThreadWorker(groupName + "-checker");
        /**
         * 绑定池不用做维护
         */
        if (!isBind) {
            startCheckTask();
        }
    }

    /**
     * 以指定hash方式选择线程来执行task
     */
    void execute(int hash, Runnable task) {
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

    volatile int random = 7;

    /**
     * 随机算则线程来执行task,这个随机算法在入队线程较少的时候依赖random的改变
     * <p>
     * 在threadAmountAdjust时,因为proxy的execute和stop之间是互斥的,此方法保证在同时发生shutdown时不会丢失任务
     * 在非threadAmountAdjust时,shutdown同时execute成功一个任务,这个任务可能不会被执行或terminate,造成此任务会丢失
     * 所以在需要shutdown的场合,shutdown需要和execute同步
     */
    @Override
    public final void execute(Runnable task) {
        int hash = (int) Thread.currentThread().getId() + random;
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

        /**
         * 启动线程0来执行任务
         */
        proxies[0].start(task);
    }

    /**
     * 支持超时操作
     * 为了提高性能,代码冗余较多
     */
    public void executeTimeout(Runnable command, long delay, TimeUnit unit) {
        long timeoutInMillions = unit.toMillis(delay);

        int hash = (int) Thread.currentThread().getId() + random;
        int _mount = amount;
        while (_mount > 0) {
            if (proxies[hash % _mount].executeTimeout(command, timeoutInMillions)) {
                return;
            }
            _mount--;
        }

        proxies[0].startTimeout(command, timeoutInMillions);
    }

    /**
     * 日常运维任务
     */
    final CheckTask startCheckTask() {

        return new CheckTask(maintainThread) {

            @Override
            void changeToCheckThread() {
                //取消worker争抢执行check
                for (int i = 0; i < maxAmount; i++) {
                    ScheduledThreadWorker worker = proxies[i].worker;
                    if (worker != null) {
                        worker.clearCheckTask();
                    }
                }
                //创建新的check thread
                super.changeToCheckThread();
            }

            @Override
            void changeToWorkers() {
                //关掉check thread
                super.changeToWorkers();
                //设置worker争抢check
                for (int i = 0; i < maxAmount; i++) {
                    ScheduledThreadWorker worker = proxies[i].worker;
                    if (worker != null) {
                        worker.setCheckTask(this);
                    }
                }
            }

            long check(AtomicBoolean returnIfInCheckThread) {
                if (random++ > 10000) random = 1;

                long nextInterval;
                /**
                 * 和下面的两个{@link WorkerGroup#shutdown()}方法互斥,因为shutdown方法也是改变alreadyShutDown flag和进行stop
                 * 所以同步关键字要把alreadyShutDown和start和stop操作(check)都包括进去
                 */
                synchronized (WorkerGroup.this) {
                    if (alreadyShutDown) {
                        shutdownCheckThread();
                        //返回,不再有下一次调度
                        return minInterval;
                    }
                    //调用check业务
                    nextInterval = toCheck();
                }

                if (changeCheckThread) {
                    if (amount == maxAmount) {
                        returnIfInCheckThread.set(false);
                    } else {
                        returnIfInCheckThread.set(true);
                    }
                }

                if (random++ > 10000) random = 1;

                return nextInterval;
            }

            volatile long startLazyTime = 0;
            volatile int queueSizeSum = 0;

            /**
             * 1.新增(start)和收缩(stop)线程
             * 2.生成重平均任务
             */
            final long toCheck() {
                //空闲的ThreadProxy
                List<ThreadProxy> idleList = new ArrayList<>();
                //不空闲的ThreadProxy
                List<ThreadProxy> busyList = new ArrayList<>();
                //所有线程的任务总数
                queueSizeSum = 0;

                for (int i = 0; i < maxAmount; i++) {
                    ThreadProxy proxy = proxies[i];
                    //注意queueSize取的是线程池中已经入队还没有完成的任务数,包括当前正在执行的任务
                    int queueSize = proxy.tempQueueSize = proxy.queueSize();
                    if (queueSize == 0) {
                        ScheduledThreadWorker idleWorker = proxy.worker;
                        if (idleWorker != null) {
                            idleList.add(proxy);
                        }
                    } else {
                        //System.err.print(i + "###" + queueSize + " , ");
                        busyList.add(proxy);
                    }

                    queueSizeSum += queueSize;
                }

                //System.err.println("         " + System.currentTimeMillis());

                int _amount = amount;

                if (shrinkThreadPool) {
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
                }


                /**
                 * 因为后添加的线程都是右边线程,所以左边的旧线程积累的任务通常较多
                 * 所以在线程数达到最大之后,而且当某个线程队列一直为0的时候,开始重平衡的操作
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
                        if (i < idleList.size()) {
                            //忙的线程匹配闲的线程
                            ThreadProxy idleProxy = idleList.get(i);
                            ThreadProxy busyProxy = busyList.get(i);
                            ScheduledThreadWorker idleWorker = idleProxy.worker;
                            //和shutdown互斥,不可能为null
                            if (idleWorker != null) {
                                Runnable rebalanceTask = idleProxy.rebalanceTask(idleWorker, busyProxy.index, busyProxy.tempQueueSize);
                                idleProxy.execute(rebalanceTask);
                            }
                        }
                    }//~for
                }//~if

                return amount == maxAmount ? idleInterval : minInterval;
            }

        };
    }


    private volatile boolean alreadyShutDown = false;
    private volatile Stream<ScheduledThreadWorker> terminatingWorker;

    /**
     * 这个方法不阻塞
     * shutdown是一个个进行的,按照proxy start顺序的逆序
     */
    private final Stream<ScheduledThreadWorker> toShutdown(boolean isShutdownNow) {
        List<ThreadProxy> reverseProxies = Arrays.asList(proxies);
        //按照线程增加顺序的倒叙
        Collections.reverse(reverseProxies);

        terminatingWorker = reverseProxies.stream()
                .map(proxy -> proxy.stop(isShutdownNow))
                .filter(worker -> worker != null);

        return terminatingWorker;

    }

    @Override
    public synchronized void shutdown() {
        boolean firstCall = false;
        if (!alreadyShutDown) {
            alreadyShutDown = true;
            toShutdown(false);
            firstCall = true;
        }

        if (firstCall) {
            terminatingWorker.count();
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        boolean firstCall = false;
        synchronized (this) {
            if (!alreadyShutDown) {
                alreadyShutDown = true;
                toShutdown(true);
                firstCall = true;
            }
        }

        return firstCall ? terminatingWorker
                //getTerminatedTask这里会稍许阻塞
                .flatMap(worker -> worker.getTerminatedTask().stream()).collect(Collectors.toList())
                : null;
    }

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
            long[] remainTime = new long[]{unit.toMillis(timeout)};

            boolean re = terminatingWorker.allMatch(worker -> {
                try {
                    long start = System.currentTimeMillis();
                    if (worker.awaitTermination(remainTime[0], TimeUnit.MILLISECONDS)) {
                        remainTime[0] -= (System.currentTimeMillis() - start);
                        return remainTime[0] > 0;
                    }
                } catch (InterruptedException e) {
                    es[0] = e;
                }

                return false;
            });

            if (es[0] != null) {
                throw es[0];
            }

            return re;
        }
    }

    /**
     * 因为调度任务没有重平均,所以采用随机数来分配线程,这样会降低入队性能
     * 调度任务在各业务中调度任务相对较少
     */
    ThreadProxy randomProxy() {
        int hash = ThreadLocalRandom.current().nextInt(4096);
        return proxies[hash % amount];
    }

    public int getQueueSize() {
        int sum = 0;
        for (int i = 0; i < maxAmount; i++) {
            ThreadProxy proxy = proxies[i];
            sum += proxy.queueSize();
        }
        return sum;
    }


    /**
     * 此类包装一个单线程的线程池
     */
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

        volatile ScheduledThreadWorker worker;

        void start() {
            final ReentrantReadWriteLock.WriteLock lock = writeLock;
            lock.lock();
            try {
                if (worker == null) {
                    /**
                     * 会不会和shutdown流程冲突
                     * 这里和stop互斥,进入这里意味着肯定已经stop(在stop之前进入什么都不做)
                     * 而stop发生在alreadyShutDown标志位之后,也就是说肯定会抛出异常
                     */
                    if (alreadyShutDown) {
                        throw new RejectedExecutionException();
                    }
                    worker = new ScheduledThreadWorker(groupName + "-" + index);
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

        void startTimeout(Runnable task, long timeoutInMillions) {
            while (!executeTimeout(task, timeoutInMillions)) {
                start();
            }
        }

        /**
         *
         */
        ScheduledThreadWorker stop(boolean isShutDownNow) {
            ScheduledThreadWorker _worker;

            final ReentrantReadWriteLock.WriteLock lock = writeLock;
            lock.lock();
            try {
                _worker = worker;
                if (_worker != null) {
                    amount--;
                    worker = null;
                } else {
                    return null;
                }
            } finally {
                lock.unlock();
            }

            if (isShutDownNow) {
                _worker.shutdownNow(false);
            } else {
                _worker.shutdown();
            }


            return _worker;
        }

        /**
         * 如果内部的线程池已经或正在销毁,就返回false
         */
        boolean execute(Runnable task) {
            final ReentrantReadWriteLock.ReadLock lock = readLock;
            if (shrinkThreadPool) lock.lock();
            try {
                ScheduledThreadWorker _executor = worker;
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
                if (shrinkThreadPool) lock.unlock();
            }
        }

        boolean executeTimeout(Runnable task, long timeoutInMillions) {
            final ReentrantReadWriteLock.ReadLock lock = readLock;
            if (shrinkThreadPool) lock.lock();
            try {
                ScheduledThreadWorker _executor = worker;
                if (_executor != null) {
                    try {
                        _executor.executeTimeout(task, timeoutInMillions, maintainThread);
                        return true;
                    } catch (RejectedExecutionException e) {
                        e.printStackTrace();
                    }
                }

                return false;
            } finally {
                if (shrinkThreadPool) lock.unlock();
            }
        }

        /**
         * 给check线程在check方法内部使用,不用同步
         */
        int tempQueueSize;

        Runnable rebalanceTask(ScheduledThreadWorker oldWorker, int busyIndex, int busyQueueSize) {
            /**
             * 如果此任务被成功execute,并成功run,有可能处于销毁时的假运行状态
             * 即使worker没有被shutdown,任务在run的时候worker也随时会shutdown
             */
            return () -> {
                ScheduledThreadWorker busyWorker = proxies[busyIndex].getWorker();
                if (busyWorker == null || oldWorker != worker) return;
                /**
                 * 能进入下面代码,说明worker shutdown在这之后发生
                 * 这个任务运行在worker shutdown之前,不是销毁时假运行的任务
                 */
                //从繁忙线程分担的任务数,当繁忙线程队列中只有一个任务的时候,也要将这个任务拿过来
                int shareSize = Math.round(busyQueueSize / 2f);
                //从繁忙线程中窃取任务,此任务已经被取出,要尽量确保其不会丢失
                List<Runnable> tasks = busyWorker.stealTask(shareSize);
                if (tasks.isEmpty()) return;

                boolean rebalanced = false;
                /**
                 * 和stop互斥,也就是和运维线程的stop操作和shutdown操作互斥
                 */
                final ReentrantReadWriteLock.ReadLock lock = readLock;
                if (shrinkThreadPool) lock.lock();
                try {
                    if (worker != null) {
                        try {
                            worker.executeBatch(tasks);
                            rebalanced = true;
                        } catch (Exception e) {
                            rebalanced = false;
                        }
                    }
                } finally {
                    if (shrinkThreadPool) lock.unlock();
                }

                /**
                 * 由于在上面return语句和lock语句之间,worker被shutdown了,可能性不大,但不会没有
                 * 1.executed失败如果是group shutdown引起的
                 * 此时alreadyShutDown(group shutdown在各个worker shutdown之前)肯定是true
                 * 2.worker被运维线程shutdown了,目前是此线程真运行的最后一个任务,将这些任务再分散入到group的所有线程队列中去
                 */
                Runnable task = null;
                if (!rebalanced) {
                    Iterator<Runnable> itor = tasks.iterator();
                    if (!alreadyShutDown) {
                        while (itor.hasNext()) {
                            task = itor.next();
                            try {
                                WorkerGroup.this.execute(task);
                                random++;
                                task = null;
                            } catch (Exception e) {
                                break;
                            }
                        }
                    }

                    /**
                     * 如果在分散插入到group的过程中,group shutdown导致插入报错
                     * 此时仍是真运行的最后一个任务,将这这些任务直接插入oldWorker的队列中,可以确保这些任务被正确回收
                     * 此时worker已经失效了,所以要用oldWorker
                     */
                    ConcurrentLinkedQueue<Runnable> queue = oldWorker.queue;
                    int offerSize = 0;
                    //插入上面报错的那个任务
                    if (task != null) {
                        queue.offer(task);
                        offerSize++;
                    }

                    while (itor.hasNext()) {
                        task = itor.next();
                        queue.offer(task);
                        offerSize++;
                    }

                    oldWorker.signalExecuteCommonTask(offerSize);
                }//~ not rebalanced


                //System.err.println("----" + index + " steal from:" + busyIndex + "/计划窃取数量:" + shareSize + "/实际窃取数量" + tasks.size());
            };
        }

        int queueSize() {
            ScheduledThreadWorker _worker = worker;
            if (_worker != null) {
                return _worker.getQueueSize();
            }

            return 0;
        }

        ScheduledThreadWorker getWorker() {
            return worker;
        }
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
        }
    }

}























