package bao.study.mymq.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author baoyh
 * @since 2022/10/28 14:41
 */
public abstract class ServiceThread implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ServiceThread.class);

    protected boolean stop = false;

    private final CountDownLatch waitNode = new CountDownLatch(1);

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
            waitNode.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted " + e);
        }
    }

    public void wakeup() {
        waitNode.countDown();
    }
}
