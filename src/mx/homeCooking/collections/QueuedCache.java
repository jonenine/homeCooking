package mx.homeCooking.collections;

import mx.homeCooking.UnsafeUtil;
import sun.misc.Unsafe;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 主要作为生产者和消费者之间的缓冲使用
 */
public class QueuedCache<E> extends AbstractQueue<E> {

    static final Unsafe unsafe = UnsafeUtil.unsafe;

    static final long headSegmentOffset;
    static final long tailSegmentOffset;
    static final long readHandlerOffset;

    static {
        try {
            headSegmentOffset = unsafe.objectFieldOffset
                    (QueuedCache.class.getDeclaredField("headSegment"));
            tailSegmentOffset = unsafe.objectFieldOffset
                    (QueuedCache.class.getDeclaredField("tailSegment"));
            readHandlerOffset = unsafe.objectFieldOffset
                    (QueuedCache.class.getDeclaredField("readHandler"));
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * releasedNode-->head-->node-->node-->tail
     * head和tail move on的方向-->
     * <p>
     * 1.head节点保持node在内存用的,一旦head move on,前面的节点就会就释放
     * 2.read方法从head开始遍历链表,说以要满足head.startSequence<=readSequence
     * 3.一旦有segment消费完毕,head都会立即move on
     */
    volatile SegmentNode<E> headSegment;

    /**
     * 1.末尾的segment,也是正在写入的segment
     * 2.handler最远可以读到tailSegment的前一个segment
     * 3.tail一旦写满,要立即move on
     */
    volatile SegmentNode<E> tailSegment;

    /**
     * 一个handler最多允许读的item的数量
     * 实际会取遍历过程中第一个大于此值的seg
     */
    final int maxItemSizePerHandler;


    public QueuedCache(int maxItemSizePerHandler) {
        this.maxItemSizePerHandler = maxItemSizePerHandler;

        ArraySegmentNode node = new ArraySegmentNode(0);
        unsafe.putObjectVolatile(this, headSegmentOffset, node);
        unsafe.putObjectVolatile(this, tailSegmentOffset, node);

        ReadHandler handler = new ReadHandler(0);
        unsafe.putObjectVolatile(this, readHandlerOffset, handler);
    }

    public QueuedCache() {
        this(-1);
    }

    /**
     * 1.保持对象创建时tail的snapshot
     * 2.readSequenceBeforeSnapshot==tail snapshot的startSeq时,此handler溢出作废,需要再创建一个新的handler
     */
    private final class ReadHandler {

        final SegmentNode<E> tailSegmentSnapshot;
        /**
         * 可以理解为是下一个要写入的位置的snapshot
         * 在这之前的位置已经全部读取完毕
         */
        private final long writeSequenceSnapshot;
        /**
         * 下一个要读取的位置
         * 一旦超过writeSequenceSnapshot,这个handler就失效了,就要换一个handler了
         */
        private final AtomicLong readSequenceBeforeSnapshot;

        ReadHandler(long readSequence) {
            tailSegmentSnapshot = getTailSegmentSnapshot();
            writeSequenceSnapshot = tailSegmentSnapshot.startSequence;
            this.readSequenceBeforeSnapshot = new AtomicLong(readSequence);
        }

        /**
         * 计算readSequence,只给{@link QueuedCache#peek}和{@link QueuedCache#size}使用
         */
        long getReadSequence() {
            long seq = readSequenceBeforeSnapshot.get();
            return seq < writeSequenceSnapshot ? seq : writeSequenceSnapshot + readTailSegmentCount;
        }

        /**
         * 减少写入readSequenceBeforeSnapshot,提高性能
         */
        private volatile boolean overflow = false;

        E readConcurrently() {
            if (overflow) return null;

            long seq;
            if ((seq = readSequenceBeforeSnapshot.getAndIncrement()) < writeSequenceSnapshot) {//不可能读到tailSegmentSnapshot
                /**
                 * 找到readSeg,headSegment是谁无所谓
                 */
                SegmentNode<E> readSeg = findSegmentNode(seq);

                E e = (E) readSeg.read((int) (seq - readSeg.startSequence));

                if (readSeg.read) {
                    moveOnHeadSegment();
                }

                return e;
            } else {
                /**
                 * 此时readSequenceCopy已经溢出,再次read还会返回null
                 */
                overflow = true;
                return null;
            }
        }

        /**
         * 在handler overflow后,在lock中读取tailSegmentSnapshot的计数
         */
        private volatile int readTailSegmentCount = 0;

        /**
         * 下面的方法在lock中调用
         * {@link QueuedCache#read}
         */
        E readTailSegmentSnapshot() {
            E e = tailSegmentSnapshot.itemAt(readTailSegmentCount);

            //可能会读到tail后面的segment去
            if (e != null) {
                readTailSegmentCount++;
                if (tailSegmentSnapshot.incrementReadCount()) {
                    /**
                     * 这很有可能会让head跑到tail(tail只是上次观测时的最后一个)的前面去,
                     * 等待tail move on,再进入下面的else中创建新的handler
                     * tail都读完了,说明tail早就都写完了,也就是已经link next.会让head move on到tail的next上去
                     */
                    moveOnHeadSegment();
                }
            }

            return e;
        }
    }

    /**
     * 返回tailSegment,否则就找出满足maxItemSizePerHandler要求的segment
     */
    private final SegmentNode<E> getTailSegmentSnapshot() {
        if (maxItemSizePerHandler == -1) return tailSegment;
        int count = 0;
        SegmentNode<E> h = headSegment, p = h;

        int itemSize;
        while ((itemSize = p.itemSize) != 0 && (count += p.read ? 0 : itemSize) < maxItemSizePerHandler) {
            /**
             * itemSize>0,next肯定不是null{@link ArraySegmentNode#writeOrLink}
             */
            p = p.next;
        }
        /**
         * 1.itemSize==0 writing segment,
         * 2.count>maxItemSizeInHeap
         */

        /**
         * 1.防止maxItemSizePerHandler设置的过小,导致tailSegmentSnapshot==headSegment
         * 这样会导致不停的创建新的handler,而不去读
         * 2.如果p.next==null,p还有可能返回headSegment,此时p=head=tail,造成再次在lock中读tail
         */
        if (p == h && p.next != null) {
            p = p.next;
        }

        return p;
    }

    /**
     * readSeq肯定在headSegment.startSequence的后面
     * 1.在readHandler的read方法中,readSeq肯定在tail(writing)之前的位置中
     * 2.在peek方法中,就不一定了
     */
    private final SegmentNode<E> findSegmentNode(long readSeq) {
        SegmentNode<E> p = headSegment;
        int itemSize;
        while ((itemSize = p.itemSize) > 0 && readSeq >= p.startSequence + itemSize) {
            /**
             * itemSize>0,next肯定不是null{@link ArraySegmentNode#writeOrLink}
             */
            p = p.next;
        }
        /**
         * 1.读到writing segment
         * 2.遍历到一个已经写完的segment中
         */

        return p;
    }


    volatile ReadHandler readHandler;

    /**
     * tail实际上是writing segment,进入tail时候是要加锁的
     */
    private final ReentrantLock readInTailSnapshotLock = new ReentrantLock();

    protected final E read() {
        for (; ; ) {
            E e;
            ReadHandler handler = readHandler;
            if ((e = handler.readConcurrently()) != null) return e;
            else {
                /**
                 * handler.read返回null,handler已经overflow
                 * 下面这一段必须同步
                 */
                final ReentrantLock lock = readInTailSnapshotLock;
                lock.lock();
                try {
                    if (handler != readHandler) continue;

                    SegmentNode<E> tailSnapshot;
                    if ((tailSnapshot = handler.tailSegmentSnapshot) == tailSegment) {
                        /**
                         * 除tail(writing)外都已经读完
                         * 此时虽然handler已经overflow,并不意味着所有的读线程都读完了,除了当前线程已经返回外,可能还有其他线程正在handler.read中
                         * 下面进行一个延时等待,等待所有线程都读完,也就是等待将tailSnapshot之前全部读完,让head move on到tailSnapshot
                         * 也就是说moveOnHeadSegment方法中不能做hops优化
                         * 这个等待会让写慢读快情境下,队列的效率降低.不过这个类应对的场景就是写快读慢
                         */
                        while (headSegment != tailSnapshot) {
                            //just wait a moment
                            Thread.yield();
                        }

                        return handler.readTailSegmentSnapshot();
                    } else {
                        /**
                         * 还有可读的segment,此时readHandler已经溢出,所以要更新readHandler
                         */
                        if (handler == readHandler) {
                            /**
                             * 虽然tail已经区别于tail snapshot,,但可能tail snapshot已经被读完
                             * 造成head move on,而且head=tail,下次再读的时候还是在lock中读
                             */
                            unsafe.putObjectVolatile(this, readHandlerOffset, new ReadHandler(handler.getReadSequence()));
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    protected final void write(E e) {
        /**
         * 写tailSegment,写满后立即move on tail
         * 这里默认tailSegment就是writing segment,
         */
        SegmentNode t;
        while (!(t = tailSegment).writeOrLink(e)) {
            unsafe.compareAndSwapObject(this, tailSegmentOffset, t, t.next);
        }
    }

    /**
     * headSegment向tail移动,释放内存
     */
    private final void moveOnHeadSegment() {
        /**
         * 找到最后一个读过的seg
         */
        SegmentNode p = headSegment, h = null;
        while (p != null && p.read) {
            h = p;
            p = p.next;
        }

        if (h != null) {
            /**
             * 尽量为最后一个读过的seg的next,否则就为这个seg
             */
            h = p != null ? p : h;
            if (h != headSegment) {
                unsafe.putObjectVolatile(this, headSegmentOffset, h);
            }
        }
    }

    @Override
    public E poll() {
        return read();
    }

    @Override
    public boolean offer(E e) {
        write(e);
        return true;
    }


    /**
     * 这个类的最终目的是实现缓冲的持久化,迭代式无意义的
     */
    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    /**
     * 因为是无界队列,所以可能出现无法返回的数字
     */
    @Override
    public int size() {
        return (int) (tailSegment.getWriteSequence() - readHandler.getReadSequence());
    }


    /**
     * peek的精确含义是不从队列中拿走值的poll
     */
    @Override
    public E peek() {
        E e;
        long seq = readHandler.getReadSequence();
        do {
            SegmentNode<E> seg = findSegmentNode(seq);
            e = (E) seg.itemAt((int) (seq - seg.startSequence));
        } while (seq != (seq = readHandler.getReadSequence()));

        return e;
    }

    /*---------------------------支持一下阻塞队列------------------------------------*/
    private final ReentrantLock waitLock = new ReentrantLock();
    private final Condition notEmpty = waitLock.newCondition();
    private final AtomicInteger awaitNum = new AtomicInteger(0);

    public E take() throws InterruptedException {
        E e;
        while ((e = read()) == null) {//1
            /**
             * put方法可能会在1和4之间写入并读取awaitNum
             * 只需在4之后再读取一次即可
             */
            awaitNum.incrementAndGet();//4

            final ReentrantLock lock = waitLock;
            lock.lock();
            /**
             * signal lock可能在await lock之前,这样signal就不起作用了
             * 在进入await lock之后再读取一次即可
             */
            try {
                if((e = read()) == null){
                    notEmpty.await();
                }else{
                    return e;
                }
            } finally {
                lock.unlock();
                awaitNum.decrementAndGet();
            }
        }

        return e;
    }

    public void put(E e) {
        write(e);//2
        if (awaitNum.get() > 0) {//3
            final ReentrantLock lock = waitLock;
            lock.lock();
            try {
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }
    }


}







