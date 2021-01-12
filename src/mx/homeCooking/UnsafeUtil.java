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

    static final long[] addresses = new long[8];

    static final Unsafe unsafe;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);

            Class clazz = UnsafeUtil.class;
            for(int i=0;i<8;i++){
                addresses[i] = unsafe.objectFieldOffset
                        (clazz.getDeclaredField("random"+i));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
