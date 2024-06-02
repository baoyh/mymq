package bao.study.mymq.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author baoyh
 * @since 2022/10/28 14:41
 */
public abstract class ServiceThread implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ServiceThread.class);

    protected volatile boolean stop = false;

    private final AtomicReference<CountDownLatch> waitNode = new AtomicReference<>(new CountDownLatch(1));

    public void start() {
        log.info("Try to start service " + getServiceName());
        Thread thread = new Thread(this, getServiceName());
        thread.start();
    }

    public void shutdown() {
        stop = true;
    }

    public abstract String getServiceName();

    public void waitForRunning(long timeout) {
        try {
            waitNode.get().await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted " + e);
        }
    }

    public void waitForWakeup() {
        try {
            waitNode.get().await();
        } catch (InterruptedException e) {
            log.error("Interrupted " + e);
        }
    }

    public void reset() {
        waitNode.set(new CountDownLatch(1));
    }

    public void wakeup() {
        waitNode.get().countDown();
    }
}
