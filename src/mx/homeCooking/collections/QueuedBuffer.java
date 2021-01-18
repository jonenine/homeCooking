package mx.homeCooking.collections;

import mx.homeCooking.UnsafeUtil;
import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 主要作为生产者和消费者之间的缓冲使用
 */
public class QueuedBuffer<E> {

    static final Unsafe unsafe = UnsafeUtil.unsafe;

    static final long headSegmentOffset;
    static final long tailSegmentOffset;
    static final long readHandlerOffset;

    static {
        try {
            headSegmentOffset = unsafe.objectFieldOffset
                    (QueuedBuffer.class.getDeclaredField("headSegment"));
            tailSegmentOffset = unsafe.objectFieldOffset
                    (QueuedBuffer.class.getDeclaredField("tailSegment"));
            readHandlerOffset = unsafe.objectFieldOffset
                    (QueuedBuffer.class.getDeclaredField("readHandler"));
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 表示下一次要读的位置
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

        ReadHandler(SegmentNode<E> tailSegment, long readSequence) {
            tailSegmentSnapshot = tailSegment;
            writeSequenceSnapshot = tailSegment.startSequence;
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
     * readSeq肯定在headSegment的后面
     */
    private final SegmentNode<E> findSegmentNode(long readSeq) {
        SegmentNode<E> readSeg = headSegment;
        while (readSeq >= readSeg.startSequence + readSeg.itemSize) {
            readSeg = readSeg.next;
            //查找过程中,head move on
            if (readSeg == null) readSeg = headSegment;
        }

        return readSeg;
    }

    public E peek() {
        long readSeq;
        E e;
        /**
         * 用自旋来更精确的实现peek
         */
        do {
            readSeq = readSequence.get();
            SegmentNode<E> readSeg = findSegmentNode(readSeq);
            e = (E) readSeg.itemAt((int) (readSeq - readSeg.startSequence));
        } while (readSeq != readSequence.get());

        return e;
    }

    /**
     * 因为tail实际上是writing segment,所以进入tail读取的时候是要加锁的
     */
    private final ReentrantLock closeTailLock = new ReentrantLock();

    volatile ReadHandler readHandler;

    protected final E read() {
        for (; ; ) {
            E e;
            ReadHandler handler = readHandler;
            if ((e = handler.read()) != null)  return e;
            else {
                /**
                 * handler.read返回null,handler已经overflow
                 * 下面这一段必须同步
                 */
                final ReentrantLock lock = closeTailLock;
                lock.lock();
                try {
                    SegmentNode<E> tailSnapshot;
                    if ((tailSnapshot = handler.tailSegmentSnapshot) == tailSegment) {//在消费的过程中,write没有让tail move on
                        /**
                         * 并不意味着所有的读线程都读完了
                         * 也就是,除了当前线程已经返回外,可能还有其他线程正在handler.read中
                         * 下面进行一个延时等待,等待所有线程都读完
                         */
                        while (readSequence.get() < handler.writeSequenceSnapshot) {
                            //just wait a moment
                            Thread.yield();
                        }

                        /**
                         * handler.read返回null,说明已经handler已经失效了
                         * 在lock中直接读
                         */
                        e = tailSnapshot.itemAt((int) (readSequence.get() - tailSnapshot.startSequence));

                        //可能会读到tail后面的segment去
                        if (e != null) {
                            readSequence.incrementAndGet();
                            if (tailSnapshot.incrementReadCount()) {
                                /**
                                 * 这很有可能会让head跑到tail的前面去,而此时handler已经溢出,无用
                                 * 等待tail move on,再进入下面的else中创建新的handler
                                 * tail都读完了,说明tail早就都写完了,也就是已经link next.会让head move on到tail的next上去
                                 */
                                moveOnHeadSegment();
                            }
                        }

                        return e;
                    }else{
                        /**
                         * tailSegment相对于tailSegmentSnapshot已经move on,readHandler已经溢出,所以要更新readHandler
                         */
                        if (handler == readHandler) {
                            /**
                             * 可能正在读tail的过程中,发生了tailSegment move on,此时readSequence可能已经读了几个值了
                             */
                            long readSeq =  Math.max(readSequence.get(),handler.writeSequenceSnapshot);
                            unsafe.compareAndSwapObject(this, readHandlerOffset, handler,
                                    new ReadHandler(getTailSegmentSnapshot(),readSeq));
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
         * 写tailSegment,直到写满后再move on tail
         * 这里默认tailSegment就是writing segment,不再优化为多读少写
         */
        SegmentNode t;
        while (!(t = tailSegment).writeOrLink(e)) {
            unsafe.compareAndSwapObject(this, tailSegmentOffset, t, t.next);
        }
    }


    public E poll() {
        return read();
    }

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
     */
    volatile SegmentNode<E> headSegment;


    /**
     * headSegment向tail移动,释放内存
     */
    private final void moveOnHeadSegment() {
        /**
         * 找到最后一个读过的seg
         */
        SegmentNode headSeg, h = null;
        while ((headSeg = headSegment).read && headSeg.next != null) {
            h = headSeg;
        }

        if (h != null && h != headSegment) {
            unsafe.putObject(this, headSegmentOffset, h);
        }
    }

    /**
     * 一个handler最多允许读的item的数量
     * 这个值可以设置的小一些用来支持持久化
     */
    final int maxReadItemSize;

    private SegmentNode<E> getTailSegmentSnapshot() {
        if (maxReadItemSize == -1) return tailSegment;
        int count = 0;
        SegmentNode<E> h = headSegment;
        int itemSize;
        while ((itemSize = h.itemSize) != 0 && (count += itemSize) < maxReadItemSize) {
            //itemSize不为0,next肯定不是null
            h = h.next;
        }

        return h.next == null ? h : h.next;
    }


    public QueuedBuffer(int maxReadItemSize) {
        this.maxReadItemSize = maxReadItemSize;
        headSegment = tailSegment = new ArraySegmentNode(0);
        readHandler = new ReadHandler(tailSegment, 0);
    }

    public QueuedBuffer() {
        this(-1);
    }

}







