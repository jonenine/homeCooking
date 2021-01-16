package mx.homeCooking.collections;


import mx.homeCooking.UnsafeUtil;
import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 主要作为生产者和消费者这件的缓冲使用
 */
public class QueuedBuffer<E> {

    static final int arraySize = 200;

    static final Unsafe unsafe = UnsafeUtil.unsafe;

    static final long headSegmentOffset;
    static final long tailSegmentOffset;

    static {
        try {
            headSegmentOffset = unsafe.objectFieldOffset
                    (QueuedBuffer.class.getDeclaredField("headSegment"));
            tailSegmentOffset = unsafe.objectFieldOffset
                    (QueuedBuffer.class.getDeclaredField("tailSegment"));
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    static class ArraySegmentNode<E> {

        static final long writeCountOffset;
        static final long readCountOffset;
        static final long nextOffset;

        static {
            try {
                writeCountOffset = unsafe.objectFieldOffset
                        (ArraySegmentNode.class.getDeclaredField("writeCount"));
                readCountOffset = unsafe.objectFieldOffset
                        (ArraySegmentNode.class.getDeclaredField("readCount"));
                nextOffset = unsafe.objectFieldOffset
                        (ArraySegmentNode.class.getDeclaredField("next"));

            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            }
        }

        final long startSequence;
        final long nextStartSequence;
        final int size;
        final Object[] array;

        ArraySegmentNode(long startSequence, int size) {
            this.startSequence = startSequence;
            this.nextStartSequence = startSequence + size;
            this.size = size;
            this.array = new Object[size];
        }

        volatile ArraySegmentNode next;

        private final boolean linkNext(ArraySegmentNode next) {
            return unsafe.compareAndSwapObject(this, nextOffset, null, next);
        }

        volatile int writeCount = 0;

        int incrementWriteCount() {
            return unsafe.getAndAddInt(this, writeCountOffset, 1);
        }

        volatile int readCount = 0;
        volatile boolean read = false;

        boolean incrementReadCount() {
            if (unsafe.getAndAddInt(this, readCountOffset, 1) == size - 1) {
                read = true;
            }

            return read;
        }

        /**
         * 向segment写入一个元素,当返回为false时,表示此segment已经写满,但确保已经创建了next
         */
        boolean write(E e) {
            int index = incrementWriteCount();
            if (index < size) {
                array[index] = e;
                return true;
            } else {
                if (next == null) {
                    linkNext(new ArraySegmentNode(nextStartSequence, size));
                }

                return false;
            }
        }

        E read(int index) {
            if (index>=0 && index < size) {
                return (E) array[index];
            } else {
                return null;
            }
        }

    }

    final AtomicLong readSequence = new AtomicLong(0);

    class ReadHandler {

        final ArraySegmentNode<E> tailSegmentSnapshot;
        final long writeSequenceSnapshot;
        /**
         * 指向下一个要读的位置
         */
        final AtomicLong readSequenceCopy;

        ReadHandler() {
            this.tailSegmentSnapshot = QueuedBuffer.this.tailSegment;
            //注意,一定要用this.tailSegmentSnapshot而不是tailSegment
            writeSequenceSnapshot = this.tailSegmentSnapshot.startSequence;
            readSequenceCopy = new AtomicLong(readSequence.get());
        }

        volatile boolean overflow = false;

        E read() {
            if (overflow) return null;

            //返回下一个要读的位置,并将readSequence指向下一个位置
            long seqCopy = readSequenceCopy.getAndIncrement();
            if (seqCopy < writeSequenceSnapshot) {
                /**
                 * 找到readSeg,headSegment是谁无所谓
                 */
                ArraySegmentNode<E> readSeg = headSegment;
                while (seqCopy >= readSeg.nextStartSequence) {
                    readSeg = readSeg.next;
                }

                E e = (E) readSeg.read((int) (seqCopy - readSeg.startSequence));

                readSequence.getAndIncrement();
                if (readSeg.incrementReadCount()) {
                    moveOnHeadSegment();
                }

                return e;
            } else {
                /**
                 * 此时readIndex已经溢出,再次read还会返回null
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
            ReadHandler handler = readHandler;
            E e = handler.read();
            if (e != null) {
                return e;
            } else {
                final ReentrantLock lock = headCloseTailLock;
                lock.lock();
                if (handler != readHandler) continue;
                try {
                    /**
                     * 在消费的过程中,write没有让tail move on
                     */
                    ArraySegmentNode<E> tailSnapshot = handler.tailSegmentSnapshot;
                    if (tailSnapshot == tailSegment) {
                        /**
                         * 不再使用handler,在lock中直接读
                         */
                        e = tailSnapshot.read((int) (readSequence.get() - handler.writeSequenceSnapshot));
                        if (e != null) {
                            readSequence.incrementAndGet();
                            if (tailSnapshot.incrementReadCount()) {
                                /**
                                 * 这很有可能会让head跑到tail的前面去,而此时handler已经溢出,无用
                                 * 等待tail move on,再进入下面的else中创建新的handler
                                 */
                                moveOnHeadSegment();
                            }
                        }
                        return e;
                    } else {
                        /**
                         * tailSegment相对于tailSegmentSnapshot已经move on,readHandler已经溢出,所以要更新readHandler
                         * 这里和read中的readSequence自增会不会并发?
                         * 这里的代码发生在seqCopy == writeSequenceSnapshot之后
                         * 而handler.read()也有可能在seqCopy == writeSequenceSnapshot之后继续,直到readSequence自增完成
                         * 所以这里等待readSequence自增完成,防止new ReadHandler取到错误的readSequence值
                         */
                        while (readSequence.get() < readHandler.writeSequenceSnapshot) {
                            //just wait a moment
                            Thread.yield();
                        }
                        readHandler = new ReadHandler();
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    protected final void write(E e) {
        ArraySegmentNode tail;
        while (!(tail = tailSegment).write(e)) {
            unsafe.compareAndSwapObject(this, tailSegmentOffset, tail, tail.next);
        }
    }


    /**
     * head节点就是保持node在内存用的,一旦head后移,前面的节点在消费完毕之后就会就释放
     * 除此之外headSegment没有其他意义
     */
    volatile ArraySegmentNode<E> headSegment;


    /**
     * headSegment向tail移动,释放内存
     */
    private final void moveOnHeadSegment() {
        ArraySegmentNode headSeg;
        while ((headSeg = headSegment).read && headSeg.next != null) {
            /**
             * 注意可能会前后两个segment同时read
             * 更新不成功的线程退出循环
             */
            if (!unsafe.compareAndSwapObject(this, headSegmentOffset, headSeg, headSeg.next)) {
                break;
            }
        }
    }

    volatile ArraySegmentNode<E> tailSegment;

    QueuedBuffer() {
        headSegment = tailSegment = new ArraySegmentNode(0, arraySize);
        readHandler = new ReadHandler();
    }

}







