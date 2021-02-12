package mx.homeCooking;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeUtil {
    private long p01, p02, p03, p04, p05, p06, p07, p08, p09, p0a, p0b, p0c, p0d, p0e, p0f, p0g, p0h, p0i, p0j, p0k;
    private volatile long random0 = 0;
    private long p11, p12, p13, p14, p15, p16, p17, p18, p19, p1a, p1b, p1c, p1d, p1e, p1f, p1g, p1h, p1i, p1j, p1k;
    private volatile long random1 = 8;
    private long p21, p22, p23, p24, p25, p26, p27, p28, p29, p2a, p2b, p2c, p2d, p2e, p2f, p2g, p2h, p2i, p2j, p2k;
    private volatile long random2 = 16;
    private long p31, p32, p33, p34, p35, p36, p37, p38, p39, p3a, p3b, p3c, p3d, p3e, p3f, p3g, p3h, p3i, p3j, p3k;
    private volatile long random3 = 24;
    private long p41, p42, p43, p44, p45, p46, p47, p48, p49, p4a, p4b, p4c, p4d, p4e, p4f, p4g, p4h, p4i, p4j, p4k;
    private volatile long random4 = 32;
    private long p51, p52, p53, p54, p55, p56, p57, p58, p59, p5a, p5b, p5c, p5d, p5e, p5f, p5g, p5h, p5i, p5j, p5k;
    private volatile long random5 = 40;
    private long p61, p62, p63, p64, p65, p66, p67, p68, p69, p6a, p6b, p6c, p6d, p6e, p6f, p6g, p6h, p6i, p6j, p6k;
    private volatile long random6 = 48;
    private long p71, p72, p73, p74, p75, p76, p77, p78, p79, p7a, p7b, p7c, p7d, p7e, p7f, p7g, p7h, p7i, p7j, p7k;
    private volatile long random7 = 56;
    private long p81, p82, p83, p84, p85, p86, p87, p88, p89, p8a, p8b, p8c, p8d, p8e, p8f, p8g, p8h, p8i, p8j, p8k;

    /**
     * 1.一个线程实际只对应唯一一个random变量,可以去掉所有变量的volatile关键字,但去掉后,反而慢
     * 2.消除cpu缓存行的影响是有效果的,但效果较小
     * 性能和ThreadLocalRandom.current().nextInt(4096)依然有不到一倍的差距
     */
    public long random() {
        long offset = offset(Thread.currentThread().getId() % 8);
        long value = unsafe.getLongVolatile(this, offset) + 1;
        unsafe.putOrderedLong(this, offset, value);
        return Math.abs(value);
    }


    private static final long offset(long varSeg) {
        return base + varSeg * interval;
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
