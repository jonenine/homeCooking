package mx.homeCooking.collections;

import mx.homeCooking.UnsafeUtil;
import sun.misc.Unsafe;

abstract class SegmentNode<E> {

    static final Unsafe unsafe = UnsafeUtil.unsafe;

    static final long nextOffset;

    static {
        try {
            nextOffset = unsafe.objectFieldOffset
                    (SegmentNode.class.getDeclaredField("next"));
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    final long startSequence;

    SegmentNode(long startSequence) {
        this.startSequence = startSequence;
    }

    /**
     * 只有size方法使用
     */
    public long getWriteSequence(){
        return startSequence + getWriteIndex();
    }


    /*-----------------------下面三个属性也是接口的一部分-----------------------*/
    /**
     * 在写完毕之后,必须要设置itemSize
     * 比如一个自己管理内存的Segment,当到超出已经申请的内存大小时
     * write返回false(link),而且要设置itemSize
     */
    volatile int itemSize = 0;

    /**
     * 所有元素是否已经读完
     */
    volatile boolean read = false;

    volatile SegmentNode<E> next = null;

    final boolean linkNext(SegmentNode next) {
        return unsafe.compareAndSwapObject(this, nextOffset, null, next);
    }


    /**
     * 得到当前写索引,返回值的范围为0-itemSize
     */
    protected abstract int getWriteIndex();

    /**
     * 要和read属性配合起来使用
     */
    abstract boolean incrementReadCount() ;

    /**
     * 写segment的唯一方法,不允许其他方式的写
     * 向segment写入一个元素,当返回为false时,表示此segment已经写满,但确保已经创建了next
     */
    public abstract boolean writeOrLink(E e);

    /**
     * 返回当前索引位置的元素值
     */
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
