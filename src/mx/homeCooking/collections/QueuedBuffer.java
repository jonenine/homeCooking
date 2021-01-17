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
     */
    final AtomicLong readSequence = new AtomicLong(0);

    class ReadHandler {
        final SegmentNode<E> writingSegment;
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
            writingSegment = tailSegment;
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
            if (readSeq < writeSequenceSnapshot) {
                /**
                 * 找到readSeg,headSegment是谁无所谓
                 */
                SegmentNode<E> readSeg = headSegment;
                while (readSeq >= readSeg.startSequence + readSeg.itemSize) {
                    readSeg = readSeg.next;
                    //查找过程中,head move on
                    if (readSeg == null) readSeg = headSegment;
                }

                E e = (E) readSeg.read((int) (readSeq - readSeg.startSequence));

                readSequence.getAndIncrement();
                if (readSeg.incrementReadCount()) {
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

    private final ReentrantLock headCloseTailLock = new ReentrantLock();

    volatile ReadHandler readHandler;

    protected final E read() {
        for (; ; ) {
            E e;
            ReadHandler handler = readHandler;
            if ((e = handler.read()) != null) {
                return e;
            } else {
                /**
                 * handler.read返回null,handler肯定已经overflow,但并不意味着所有的读线程都读完了
                 * 也就是,除了当前线程已经返回外,可能还有其他线程正在handler.read中
                 * 下面进行一个延时等待,等待所有线程都读完
                 */
                while (readSequence.get() < handler.writeSequenceSnapshot) {
                    //just wait a moment
                    Thread.yield();
                }

                /**
                 * handler.read返回null,说明已经handler已经失效了
                 */
                final ReentrantLock lock = headCloseTailLock;

                if (handler != readHandler) continue;
                SegmentNode<E> writingSegment = handler.writingSegment;
                /**
                 * 在消费的过程中,write没有让tail move on
                 */
                if (writingSegment == tailSegment) {
                    /**
                     * 不再使用handler,在lock中直接读
                     */
                    lock.lock();
                    try {
                        e = writingSegment.read((int) (readSequence.get() - writingSegment.startSequence));
                        if (e != null) {
                            readSequence.incrementAndGet();
                            if (writingSegment.incrementReadCount()) {
                                /**
                                 * 这很有可能会让head跑到tail的前面去,而此时handler已经溢出,无用
                                 * 等待tail move on,再进入下面的else中创建新的handler
                                 */
                                moveOnHeadSegment();
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                    return e;
                } else {
                    /**
                     * tailSegment相对于tailSegmentSnapshot已经move on,readHandler已经溢出,所以要更新readHandler
                     */
                    if (handler == readHandler) {
                        unsafe.compareAndSwapObject(this, readHandlerOffset, handler, new ReadHandler(tailSegment, readSequence.get()));
                    }
                }

            }
        }
    }

    protected final void write(E e) {
        SegmentNode t;
        while (!(t = tailSegment).writeOrLink(e)) {
            unsafe.compareAndSwapObject(this, tailSegmentOffset, t, t.next);
        }
    }


    /**
     * 末尾的segment,通常是正在写入的segment
     */
    volatile SegmentNode<E> tailSegment;


    /**
     * head节点就是保持node在内存用的,一旦head后移,前面的节点在消费完毕之后就会就释放
     * 除此之外headSegment没有其他意义
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


    public QueuedBuffer() {
        headSegment = tailSegment = new ArraySegmentNode(0);
        readHandler = new ReadHandler(tailSegment, 0);
    }

}







