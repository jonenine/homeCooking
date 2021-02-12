package mx.homeCooking;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.LinkedTransferQueue;

public class UnsafeUtil {
    private int random0 = 0;
    private int random1 = 8;
    private int random2 = 16;
    private int random3 = 24;
    private int random4 = 32;
    private int random5 = 40;
    private int random6 = 48;
    private int random7 = 56;

    /**
     * 1.一个线程实际只对应唯一一个random变量,可以去掉所有变量的volatile关键字
     * 2.消除cpu缓存行的影响很小
     * 性能比ThreadLocalRandom.current().nextInt(4096)略好,最好的方式还是做进线程对象内
     */
    public long random() {
        long offset = offset(Thread.currentThread().getId() % 8);
        int value = unsafe.getInt(this, offset) + 1;
        unsafe.putInt(this, offset, value);
        return Math.abs(value);
    }

    public final void showRandoms(){
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<8;i++){
            sb.append(unsafe.getInt(this, offset(i))+",");
        }
        System.err.println(sb.toString());
    }

    private static final long offset(long varSeg) {
        return base + varSeg * interval;
    }

    static final long[] offsets = new long[8];

    public static final Unsafe unsafe;
    static final long base;
    static final long interval;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);

            Class clazz = UnsafeUtil.class;
            long lastOffset = 0;
            for (int i = 0; i < 8; i++) {
                offsets[i] = unsafe.objectFieldOffset
                        (clazz.getDeclaredField("random" + i));
                if (lastOffset != 0) {
                    if (offsets[i] - lastOffset == 4) {
                        lastOffset = offsets[i];
                    } else {
                        System.err.println("random field之间不是连续排列");
                    }
                }
            }
            //首地址
            base = offsets[0];
            //一般差距是4
            interval = offsets[1] - base;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
