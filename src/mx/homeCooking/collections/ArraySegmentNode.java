package mx.homeCooking.collections;

import mx.homeCooking.UnsafeUtil;
import sun.misc.Unsafe;


/**
 * 创建一个固定元素大小的,基于jvm对象数组的Segment
 */
class ArraySegmentNode<E> extends SegmentNode<E> {

    static final Unsafe unsafe = UnsafeUtil.unsafe;
    static final long readCountOffset;
    static final long writeIndexOffset;

    static final long base;
    static final int shift;

    static {
        try {
            readCountOffset = unsafe.objectFieldOffset
                    (ArraySegmentNode.class.getDeclaredField("readCount"));
            writeIndexOffset = unsafe.objectFieldOffset
                    (ArraySegmentNode.class.getDeclaredField("writeIndex"));
            /**
             * {@link java.util.concurrent.atomic.AtomicReferenceArray}
             */
            Class<?> aClass = Object[].class;
            base = unsafe.arrayBaseOffset(aClass);
            int scale = unsafe.arrayIndexScale(aClass);
            shift = 31 - Integer.numberOfLeadingZeros(scale);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private static long byteOffset(int i) {
        return ((long) i << shift) + base;
    }

    final static int ARRAY_SIZE = 200;

    private final Object[] array;

    ArraySegmentNode(long startSequence) {
        super(startSequence);
        this.array = new Object[ARRAY_SIZE];
    }

    /**
     * 下一个要写入的索引
     * writeIndex-1未必已经写入,但终会写入
     */
    private volatile int writeIndex = 0;

    @Override
    public int getWriteIndex() {
        return writeIndex >= ARRAY_SIZE ? (ARRAY_SIZE - 1) : writeIndex;
    }

    @Override
    public boolean writeOrLink(E e) {
        /**
         * 注意下面是index先+1,再写入数组,这会导致writeIndex不准确
         * 不过目前只有cache的size方法使用了writeIndex
         */
        int index = unsafe.getAndAddInt(this, writeIndexOffset, 1);
        if (index < ARRAY_SIZE) {
            /**
             * 写volatile
             */
            unsafe.putObjectVolatile(array, byteOffset(index), e);
            return true;
        } else {
            if (next == null) {
                /**
                 * 注意要先link,再设置itemSize
                 * {@link  QueuedCache#getTailSegmentSnapshot,QueuedCache#findSegmentNode}
                 */
                linkNext(new ArraySegmentNode(startSequence + ARRAY_SIZE));
                this.itemSize = ARRAY_SIZE;
            }

            return false;
        }
    }

    @Override
    public E itemAt(int index) {
        if (index >= 0 && index < ARRAY_SIZE) {
            /**
             * 读volatile
             */
            return (E) unsafe.getObjectVolatile(array, byteOffset(index));
        }

        return null;
    }

    /**
     * 已读计数+1,如果读的数量==size,则返回true
     */
    volatile int readCount = 0;

    public boolean incrementReadCount() {
        /**
         * 在itemSize被设置之前,就可能会读到这里,所以下面对的判断只能用ARRAY_SIZE而不能用itemSize
         */
        if (unsafe.getAndAddInt(this, readCountOffset, 1) == ARRAY_SIZE - 1) {
            read = true;
        }

        return read;
    }

    public static void main(String[] args) {
        Class<?> ak = Object[].class;
        long baseOffset = unsafe.arrayBaseOffset(ak);
        int scale = unsafe.arrayIndexScale(ak);
        int shift = 31 - Integer.numberOfLeadingZeros(scale);
        System.err.println(baseOffset + "/" + scale + "/" + shift);

        Object[] target = new Object[100];
        for (int i = 0; i < 100; i++) {
            long eleOffset = ((long) i << shift) + baseOffset;
            unsafe.putObjectVolatile(target, eleOffset, i + "哈哈");
        }

        for (int i = 0; i < 100; i++) {
            System.err.println(target[i]);
        }

    }


}
