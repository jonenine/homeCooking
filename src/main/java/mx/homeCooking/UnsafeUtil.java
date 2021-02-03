package mx.homeCooking;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeUtil {
    volatile int random0 = 0;
    volatile int random1 = 0;
    volatile int random2 = 0;
    volatile int random3 = 0;
    volatile int random4 = 0;
    volatile int random5 = 0;
    volatile int random6 = 0;
    volatile int random7 = 0;

    /**
     * 假设random0-8的偏移地址如下
     * 12
     * 16
     * 20
     * 24
     * 28
     * 32
     * 36
     * 40
     * 性能和ThreadLocalRandom.current().nextInt(4096);类似或稍好,性能差别很小
     */
    public int random() {
        return Math.abs(unsafe.getAndAddInt(this, (Thread.currentThread().getId() % 8 + 3) * 4, 1));
    }

    /**
     * 和上面个多个随机数不同,下面是多线程取一个随机数,性能和多个类似
     * 多个cpu写一个内存地址,不争抢吗
     */
    private int nextRandom() {
        int r = unsafe.getInt(this, 12L);
        unsafe.putOrderedInt(this, 12L, r > 65535 ? 0 : r + 1);
        return r;
    }

    static final long[] offsets = new long[8];

    public static final Unsafe unsafe;
    static final long firstOffset;
    static final long modInterval;

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
            firstOffset = offsets[0];
            modInterval = firstOffset / 4;
            if (modInterval != 3) {
                System.err.println("mod后面加的数字不对");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
