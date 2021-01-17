package mx.homeCooking.collections;

/**
 * 创建一个固定元素大小的,基于jvm对象数组的Segment
 */
class ArraySegmentNode<E> extends SegmentNode<E> {

    static final long writeCountOffset;
    static final long readCountOffset;
    static final long nextOffset;

    static {
        try {
            writeCountOffset = QueuedBuffer.unsafe.objectFieldOffset
                    (ArraySegmentNode.class.getDeclaredField("writeCount"));
            readCountOffset = QueuedBuffer.unsafe.objectFieldOffset
                    (ArraySegmentNode.class.getDeclaredField("readCount"));
            nextOffset = QueuedBuffer.unsafe.objectFieldOffset
                    (ArraySegmentNode.class.getDeclaredField("next"));

        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    final static int arraySize = 200;

    private final Object[] array;

    ArraySegmentNode(long startSequence) {
        super(startSequence);
        this.array = new Object[arraySize];
    }


    private final boolean linkNext(ArraySegmentNode next) {
        return QueuedBuffer.unsafe.compareAndSwapObject(this, nextOffset, null, next);
    }

    volatile int writeCount = 0;


    @Override
    public int getAndIncrementWriteCount() {
        return QueuedBuffer.unsafe.getAndAddInt(this, writeCountOffset, 1);
    }

    volatile int readCount = 0;


    @Override
    public boolean incrementReadCount() {
        if (QueuedBuffer.unsafe.getAndAddInt(this, readCountOffset, 1) == itemSize - 1) {
            read = true;
        }

        return read;
    }


    @Override
    public boolean writeOrLink(E e) {
        int index = getAndIncrementWriteCount();
        if (index < itemSize) {
            array[index] = e;
            return true;
        } else {
            if (next == null) {
                this.itemSize = arraySize;
                linkNext(new ArraySegmentNode(startSequence + arraySize));
            }

            return false;
        }
    }

    @Override
    public E read(int index) {
        if (index >= 0 && index < itemSize) {
            return (E) array[index];
        } else {
            return null;
        }
    }

}
