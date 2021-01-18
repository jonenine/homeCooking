package mx.homeCooking.collections;

import mx.homeCooking.UnsafeUtil;
import sun.misc.Unsafe;

/**
 * 创建一个固定元素大小的,基于jvm对象数组的Segment
 */
class ArraySegmentNode<E> extends SegmentNode<E> {

    static final Unsafe unsafe = UnsafeUtil.unsafe;
    static final long nextOffset;

    static {

        try {
            nextOffset = unsafe.objectFieldOffset
                    (ArraySegmentNode.class.getDeclaredField("next"));
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    final static int ARRAY_SIZE = 200;

    private final Object[] array;

    ArraySegmentNode(long startSequence) {
        super(startSequence);
        this.array = new Object[ARRAY_SIZE];
    }


    private final boolean linkNext(ArraySegmentNode next) {
        return unsafe.compareAndSwapObject(this, nextOffset, null, next);
    }

    @Override
    public boolean writeOrLink(E e) {
        int index = getAndIncrementWriteIndex();
        if (index < itemSize) {
            array[index] = e;
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
    public E itemAt(int index){
        if (index >= 0 && index < itemSize) {
            return (E) array[index];
        }

        return null;
    }



}
