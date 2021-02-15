package mx.homeCooking;

import sun.misc.Contended;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeUtil {
    @Contended
    private int random0 = 1;
    @Contended
    private int random1 = 2;
    @Contended
    private int random2 = 5;
    @Contended
    private int random3 = 8;
    @Contended
    private int random4 = 17;
    @Contended
    private int random5 = 32;
    @Contended
    private int random6 = 61;
    @Contended
    private int random7 = 128;

    /**
     * 1.一个线程实际只对应唯一一个random变量,可以去掉所有变量的volatile关键字
     * 2.消除cpu缓存行的影效果不大(貌似有一些效果),当前cpu为i5-10210
     * 性能比ThreadLocalRandom.current().nextInt(4096)略好
     */
    public int random() {
        /**
         * 分散到多个变量上,不分散会很慢,只要分散到两个速度就会翻倍
         * 即使不考虑同步,多线程读写普通变量还是有成本的
         */
        long offset = offset(Thread.currentThread().getId() % 8);
        /**
         * +2比+1要快,在吞吐量小的时候快的还很明显,有意思,
         * 加2后奇数合适奇数,偶数还是偶数,减少不同线程入队到同一个worker的几率
         */
        int value = unsafe.getInt(this, offset) + 2;
        //性能主要消耗在写变量上面,只要写变量就会慢下来,估计和写在什么地方关系不大
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
