package bao.study.mymq.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author baoyh
 * @since 2022/10/28 14:41
 */
public abstract class ServiceThread implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ServiceThread.class);

    public void start() {
        log.info("Try to start service " + getServiceName());
        Thread thread = new Thread(this, getServiceName());
        thread.start();
    }

    public abstract String getServiceName();
}
