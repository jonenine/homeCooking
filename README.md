# homeCooking
high performance thread pool

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Java的线程池ThreadPoolExecutor，采用多个线程和一个阻塞队列搭配，无论是任务入队还是工作线程从队列获取任务，其同步成本都很高。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;比如我们写一个测试用例，对线程池进行压测。入队的任务很简单，就是Atomic变量自增，采用多个独立线程入队。在JProfiler上观察测试程序的执行状况，入队线程和工作线程基本都是红色的，线程基本都是处于blocking状态中，java进程占用操作系统核心较高，程序的吞吐量提升不上去。

![](media/be598fa29c141a38ce2476fadada07d1.png)

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;其实，这主要是线程池中多线程争抢单个队列造成的。最好的(也是唯一的)解决方法就是将一个队列变成多个队列，比如每个工作线程都有自己的队列，线程池变成worker-group结构。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Java标准库线程池另外一个缺陷就是调度池的入队速度太慢，即使不考虑多线程争抢的问题，也是慢的可以。看一下ScheduledThreadPoolExecutor\#execute的实现就可以找到原因

    public void execute(Runnable command) {
    
         schedule(command, 0, NANOSECONDS);
    
    }

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;原来如此，即使非调度任务也要入到其内部实现的优先级队列中。这个队列不但数据结构复杂，而且读写都上同一把锁，在多线程争抢的情况下必然很慢。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;能不能自己实现一个线程池来改变JDK标准库线程池的这两个问题呢？无论对于普通任务还是调度任务，都能实现低同步成本高吞吐量的入队和执行。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在最近这一段的闲暇时间，作者实现了一个worker-group结构的线程池，取得了不错结果，就目前跑分来看比JDK标准库线程池性能提高很多。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;采取多队列结构的还有标准库的forkJoinPool。如果说forkJoinPool的代码相当于大饭店精心烹饪的一道“佛跳墙”，那我这个野生程序员写的代码，也就相当于一盘家常的“西红柿炒鸡蛋”。这也是这个项目名称“homeCooking”的意义所在。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;下面是homeCooking线程池在jprofiler上的测试情况

![](media/bf2e90d769f8c88eb87cf663a500d22a.png)

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;可以看到线程入队时还是有零星的红色，但整体阻塞的情况已经好了很多

**测试用例1:**

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;下面是和jdk标准库普通线程池（Executors.newFixedThreadPool）和fork-join-pool的比对情况

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;8个入队线程，每线程入队300万任务，每个任务都是简单的原子值自增，线程池内线程数(core
pool Size)也是8个。一共运行20次，取后10次的平均成绩。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;测试平台：

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1.ubuntu18+e3 1231v3

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2.win10+i5 10210u

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;两个平台测试成绩相似，仅做参考


|                      | homeCooking | JDK普通线程池 | fork-join-pool |
|-------------------------|-------------|---------------|----------------|
| 入队时间/完成时间(毫秒) | 1090/1099   | 4769/5494     | 1913/2998      |

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;*注：同样任务量的情况下，时间越小越好*

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;运行时发现普通JDK线程池占用系统核心较高，同时阻塞严重。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;fork-join-pool基本没有阻塞（阻塞情况最好，运行时基本都是绿色），但因其内部算法复杂，导致其跑分仍然不及homeCooking。测试中，还有一个现象就是fork-join-pool每次的成绩差距较大，就是跑分不稳定，忽高忽低。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;homeCooking一直比较平稳，在压满cpu的情况下，占系统核心较低。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**测试用例2：**

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;测试调度任务的入队能力

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;homeCooking用内部的**跳表(ConcurrentSkipListMap)**保存调度任务，同样的也是group中的每个worker都有自己的跳表。这也极大的增加了调度任务的入队能力

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;同样是8线程入队，每线程入30万任务，每个任务都是随机延迟时间调度，线程池线程数也是8个

|                  | homeCooking | ScheduledThreadPoolExecutor |
|------------------|-------------|-----------------------------|
| 入队时间（毫秒） | 230         | 1500                        |

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;*注：fork-join-pool不支持调度任务，就不比较了。*

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;测试结果：

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;可见homeCooking在入队性能上是远远超过JDK线程池的，**在cpu核心数量越多的服务器上，这个差距也越明显**。如果你的程序是计算密集型的，换用homeCooking线程池会直接起到提升性能的作用。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**用法简介：**

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;构造homeCooking线程池主要使用mx.homeCooking.workGroup.WorkerGroups类的三个工厂方法

    executor(String groupName, int coreSize)
    
    timeoutExecutor(String groupName, int coreSize)
    
    scheduledExecutor(String groupName, int coreSize)

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;前两个方法创建的是伸缩池，即线程池的任务都执行完毕，而且没有新的任务提交，**线程池的线程数量会降到1**。最后一个方法会创建一个线程数量恒定的调度池，使用方式同jdk线程池。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在伸缩池的情况下，核心线程池随着任务的数量伸缩，这样做节省了系统资源（满足自己小老百姓过日子习惯），同时也会带来一个问题，就是任务在不同worker之间会不均匀。和fork-join-pool一样，homeCooking中的空闲线程会从繁忙线程中“偷任务“来做任务的重平衡。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;homeCooking还有很多其他用法，包括调度时支持completableFuture等，等以后有时间时再发文赘述。