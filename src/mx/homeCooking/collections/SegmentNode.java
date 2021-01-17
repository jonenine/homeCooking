package mx.homeCooking.collections;

abstract class SegmentNode<E> {

    protected final long startSequence;
    protected final long nextStartSequence;
    protected final int size;

    SegmentNode(long startSequence, int size) {
        this.startSequence = startSequence;
        this.nextStartSequence = startSequence + size;
        this.size = size;
    }

    protected volatile SegmentNode<E> next;

    /**
     * 所有元素是否已经读完
     */
    protected volatile boolean read = false;

    /**
     * 返回当前要写入的位置,并且将写入位置+1,留给下一次使用
     */
    public abstract int getAndIncrementWriteCount();

    /**
     * 已读计数+1,如果读的数量==size,则返回true
     */
    public abstract boolean incrementReadCount();

    /**
     * 向segment写入一个元素,当返回为false时,表示此segment已经写满,但确保已经创建了next
     */
    public abstract boolean writeOrLink(E e);

    /**
     * 按照segment内的索引读取一个元素
     */
    public abstract E read(int index);
}
