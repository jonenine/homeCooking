package mx.homeCooking.collections;

import mx.homeCooking.UnsafeUtil;
import sun.misc.Unsafe;

abstract class SegmentNode<E> {

    static final Unsafe unsafe = UnsafeUtil.unsafe;
    static final long readCountOffset;

    static {
        try {
            readCountOffset = unsafe.objectFieldOffset
                    (ArraySegmentNode.class.getDeclaredField("readCount"));
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    protected final long startSequence;
    /**
     * 在写完毕之后,必须要设置itemSize
     * 比如一个自己管理内存的Segment,当到超出已经申请的内存大小时
     * write返回false(link),而且要设置itemSize
     */
    protected volatile int itemSize = 0;

    SegmentNode(long startSequence) {
        this.startSequence = startSequence;
    }

    protected volatile SegmentNode<E> next;

    /**
     * 所有元素是否已经读完
     */
    protected volatile boolean read = false;


    /**
     * 已读计数+1,如果读的数量==size,则返回true
     */
    volatile int readCount = 0;

    public boolean incrementReadCount() {
        if (unsafe.getAndAddInt(this, readCountOffset, 1) == itemSize - 1) {
            read = true;
        }

        return read;
    }

    /**
     * 向segment写入一个元素,当返回为false时,表示此segment已经写满,但确保已经创建了next
     */
    public abstract boolean writeOrLink(E e);

    public abstract E itemAt(int index);

    /**
     * 按照segment内的索引读取一个元素
     */
    public E read(int index) {
        E e = itemAt(index);
        if (e != null) {
            incrementReadCount();
            return e;
        } else {
            throw new IndexOutOfBoundsException("index:" + index + "is out of bound!");
        }
    }
}
