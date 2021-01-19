package mx.homeCooking.collections;

import mx.homeCooking.UnsafeUtil;
import sun.misc.Unsafe;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
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
     * 表示下一次要读的位置,此值可能会落后实际读的位置,会使得依赖于这个值的方法返回的都未必是精确值
     * readSequence最大可以读到的值为tailSegmentSnapshot的prev的segment的值
     */
    final AtomicLong readSequence = new AtomicLong(0);

    /**
     * 保持对象创建时tail的snapshot和readSequence的一个副本
     * readSequenceCopy永远自增
     * 当readSequenceCopy==tail snapshot的startSeq时,此handler溢出作废,需要再创建一个新的handler
     */
    private final class ReadHandler {
        final SegmentNode<E> tailSegmentSnapshot;
        /**
         * 可以理解为是下一个要写入的位置的snapshot
         * 在这之前的位置已经全部读取完毕
         */
        final long writeSequenceSnapshot;
        /**
         * readSequence的副本,一旦超过writeSequenceSnapshot,这个handler就失效了,就要换一个handler了
         */
        final AtomicLong readSequenceCopy;

        ReadHandler(long readSequence) {
            tailSegmentSnapshot = getTailSegmentSnapshot();
            writeSequenceSnapshot = tailSegmentSnapshot.startSequence;
            readSequenceCopy = new AtomicLong(readSequence);
        }

        volatile boolean overflow = false;

        E read() {
            if (overflow) return null;

            /**
             * 返回当前要读的位置,并将readSequence指向下一个位置
             */
            long readSeq = readSequenceCopy.getAndIncrement();
            //不可能读到tail snapshot,也就是writingSegment
            if (readSeq < writeSequenceSnapshot) {
                /**
                 * 找到readSeg,headSegment是谁无所谓
                 */
                SegmentNode<E> readSeg = findSegmentNode(readSeq);

                E e = (E) readSeg.read((int) (readSeq - readSeg.startSequence));

                readSequence.getAndIncrement();
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
    }

    /**
     * readSeq肯定在headSegment.startSequence的后面
     * 1.在readHandler的read方法中,readSeq肯定在tail之前的位置中
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

    /**
     * peek的精确含义是不从队列中拿走值的poll
     */
    @Override
    public E peek() {
        long readSeq;
        E e;
        /**
         * 用自旋来更精确的实现peek,但仅仅是更精确
         */
        do {
            readSeq = readSequence.get();
            SegmentNode<E> seg = findSegmentNode(readSeq);
            e = (E) seg.itemAt((int) (readSeq - seg.startSequence));
        } while (readSeq != readSequence.get());

        return e;
    }

    /**
     * tail实际上是writing segment,进入tail时候是要加锁的
     */
    private final ReentrantLock closeTailLock = new ReentrantLock();

    volatile ReadHandler readHandler;

    protected final E read() {
        for (; ; ) {
            E e;
            ReadHandler handler = readHandler;
            if ((e = handler.read()) != null) return e;
            else {
                /**
                 * handler.read返回null,handler已经overflow
                 * 下面这一段必须同步
                 */
                final ReentrantLock lock = closeTailLock;
                lock.lock();
                try {
                    if (handler != readHandler) continue;

                    SegmentNode<E> tailSnapshot;
                    if ((tailSnapshot = handler.tailSegmentSnapshot) == tailSegment) {
                        /**
                         * tail没有move on,就在在lock中直接读取tail
                         * 此时虽然handler已经overflow,并不意味着所有的读线程都读完了
                         * 也就是,除了当前线程已经返回外,可能还有其他线程正在handler.read中
                         * 下面进行一个延时等待,等待所有线程都读完,也就是等待readSequence变成writeSequenceSnapshot
                         * 这个等待会让写慢读快情境下,队列的效率降低.不过这个类应对的场景就是写快读慢
                         */
                        while (readSequence.get() < handler.writeSequenceSnapshot) {
                            //just wait a moment
                            Thread.yield();
                        }

                        e = tailSnapshot.itemAt((int) (readSequence.get() - tailSnapshot.startSequence));

                        //可能会读到tail后面的segment去
                        if (e != null) {
                            readSequence.incrementAndGet();
                            if (tailSnapshot.incrementReadCount()) {
                                /**
                                 * 这很有可能会让head跑到tail(tail只是上次观测时的最后一个)的前面去,
                                 * 等待tail move on,再进入下面的else中创建新的handler
                                 * tail都读完了,说明tail早就都写完了,也就是已经link next.会让head move on到tail的next上去
                                 */
                                moveOnHeadSegment();
                            }
                        }

                        return e;
                    } else {
                        /**
                         * tailSegment相对于tailSegmentSnapshot已经move on,readHandler已经溢出,所以要更新readHandler
                         */
                        /**
                         * 可能正在读tail的过程中,发生了tailSegment move on,此时readSequence可能已经读了几个值了
                         */
                        long readSeq = Math.max(readSequence.get(), handler.writeSequenceSnapshot);
                        if (handler == readHandler) {
                            /**
                             * 虽然tail已经区别于tail snapshot,,但可能tail snapshot已经被读完
                             * 造成head move on,而且head=tail,下次再读的时候还是在lock中读
                             */
                            unsafe.compareAndSwapObject(this, readHandlerOffset, handler, new ReadHandler(readSeq));
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    private SegmentNode<E> getTailSegmentSnapshot() {
        if (maxItemSizePerHandler == -1) return tailSegment;
        int count = 0;
        SegmentNode<E> h = headSegment, p = h;

        int itemSize;
        while ((itemSize = p.itemSize) != 0 && (count += itemSize) < maxItemSizePerHandler) {
            /**
             * itemSize>0,next肯定不是null{@link ArraySegmentNode#writeOrLink}
             */
            p = p.next;
        }
        /**
         * 1.itemSize==0 writing,
         * 2.第一次count>maxItemSizeInHeap
         */

        /**
         * 1.防止maxItemSizePerHandler设置的过小,导致tailSegmentSnapshot==headSegment
         * 这样会导致不停的创建新的handler,而不去读
         * 2.如果p.next==null,p还有可能返回headSegment,此时p=head=tail,造成再次在lock中读
         */
        if (p == h && p.next != null) {
            p = p.next;
        }

        return p;
    }


    protected final void write(E e) {
        /**
         * 写tailSegment,直到写满后再move on tail
         * 这里默认tailSegment就是writing segment,不再优化为多读少写
         */
        SegmentNode t;
        while (!(t = tailSegment).writeOrLink(e)) {
            unsafe.compareAndSwapObject(this, tailSegmentOffset, t, t.next);
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
     * 末尾的segment,通常是正在写入的segment
     */
    volatile SegmentNode<E> tailSegment;


    /**
     * 1.head节点保持node在内存用的,一旦head后移,前面的节点在消费完毕之后就会就释放
     * 除此之外headSegment没有其他意义
     * 2.read方法从head开始遍历链表
     * <p>
     * head.startSequence<=readSequence
     */
    volatile SegmentNode<E> headSegment;


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
                unsafe.putObject(this, headSegmentOffset, h);
            }
        }
    }

    /**
     * 一个handler最多允许读的item的数量
     * 实际会取遍历过程中第一个大于此值的seg
     */
    final int maxItemSizePerHandler;


    public QueuedCache(int maxItemSizePerHandler) {
        this.maxItemSizePerHandler = maxItemSizePerHandler;
        headSegment = tailSegment = new ArraySegmentNode(0);
        readHandler = new ReadHandler(0);
    }

    public QueuedCache() {
        this(-1);
    }

    /**
     * 无法迭代
     *
     * @return
     */
    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    /**
     * 因为是无界队列,所以可能出现无法返回的数字
     * 即使在正整数的范围内,返回的也未必精确
     * 所以这个方法意义不大
     */
    @Override
    public int size() {
        return (int) (tailSegment.startSequence + tailSegment.writeIndex - readSequence.get());
    }

}







