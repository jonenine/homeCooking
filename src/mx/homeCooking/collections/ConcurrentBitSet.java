package mx.homeCooking.collections;

import java.util.BitSet;

/**
 * 给测试用例做校验用的
 */
public class ConcurrentBitSet {

    final BitSet[] segments;
    final int segmentNbits = 32;
    final int lastSegmentNbits ;
    final int concurrencyLevel;
    final int nbits;

    /**
     * @param nbits
     */
    public ConcurrentBitSet(int nbits) {
        //尽量以一个整数为一个segment
        int concurrencyLevel = this.concurrencyLevel = (int) Math.ceil(new Double(nbits)/32);
        this.nbits = nbits;

        segments = new BitSet[concurrencyLevel];
        int lastIndex = concurrencyLevel - 1;
        for (int i = 0; i < lastIndex; i++) {
            segments[i] = new BitSet(segmentNbits);

        }
        lastSegmentNbits = nbits - lastIndex * segmentNbits;
        segments[lastIndex] = new BitSet(lastSegmentNbits);
    }


    private int getSegmentIndex(int bitIndex) {
        return bitIndex / segmentNbits;
    }

    public boolean get(int bitIndex) {
        int segIndex = getSegmentIndex(bitIndex);
        int start = segIndex * segmentNbits;
        BitSet bitSet = segments[segIndex];
        synchronized (bitSet) {
            return bitSet.get(bitIndex - start);
        }
    }

    public void set(int bitIndex) {
        int segIndex = getSegmentIndex(bitIndex);
        int start = segIndex * segmentNbits;
        BitSet bitSet = segments[segIndex];
        synchronized (bitSet) {
            bitSet.set(bitIndex - start);
        }
    }

    public int nextClearBit(int fromIndex) {
        int segIndex = getSegmentIndex(fromIndex);
        int start = segIndex * segmentNbits;
        fromIndex = fromIndex - start;

        while (segIndex < concurrencyLevel) {
            BitSet bitSet = segments[segIndex];
            int res;
            synchronized (bitSet) {
                res = bitSet.nextClearBit(fromIndex);
            }
            fromIndex = 0;
            /**
             * 当都置为true时,nextClearBit会返回位宽度
             */
            if (res == segmentNbits) {
                segIndex++;
                start += segmentNbits;
            } else {
                /**
                 * 当都设置为true的时候,如果nbits不能整除concurrencyLevel
                 */
                return res + start;
            }
        }

        /**
         * 当都设置为true的时候,如果nbits可以整除concurrencyLevel
         */
        return nbits;
    }

    void showStatus() {
        StringBuilder sb = new StringBuilder();
        int lastIndex = concurrencyLevel - 1;
        for (int i = 0; i < concurrencyLevel; i++) {
            BitSet seg = segments[i];
            int l = i < lastIndex ? segmentNbits : nbits - lastIndex * segmentNbits;
            for (int j = 0; j < l; j++) {
                sb.append(seg.get(j) ? "1" : "0");
            }
            sb.append("|");
        }
        System.err.println(sb.toString());
    }

    public static void main(String[] args) {
        ConcurrentBitSet cbs = new ConcurrentBitSet(177);
        for (int i = 0; i < 77; i++) {
            cbs.set(i);
            cbs.showStatus();
            System.err.println(cbs.nextClearBit(0));
        }

    }

}
