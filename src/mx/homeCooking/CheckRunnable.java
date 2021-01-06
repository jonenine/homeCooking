package mx.homeCooking;

import java.util.concurrent.atomic.AtomicLong;

abstract class CheckRunnable {

    /**
     * 返回多少毫秒之后,进行下次check
     */
    abstract long run();

    final AtomicLong nextCheckTime = new AtomicLong(0);
}
