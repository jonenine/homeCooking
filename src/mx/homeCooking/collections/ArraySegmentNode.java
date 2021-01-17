package mx.homeCooking.collections;

/**
 * 应该导出一个接口
 * 内存压缩型和持久化(合并)型
 * 这个代码的合理性,应该在这个接口上探讨
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

    private final Object[] array;

    ArraySegmentNode(long startSequence, int size) {
        super(startSequence, size);
        this.array = new Object[size];
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
        if (QueuedBuffer.unsafe.getAndAddInt(this, readCountOffset, 1) == size - 1) {
            read = true;
        }

        return read;
    }


    @Override
    public boolean writeOrLink(E e) {
        int index = getAndIncrementWriteCount();
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

    @Override
    public E read(int index) {
        if (index >= 0 && index < size) {
            return (E) array[index];
        } else {
            return null;
        }
    }

}
