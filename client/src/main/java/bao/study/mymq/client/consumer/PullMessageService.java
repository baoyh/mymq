package bao.study.mymq.client.consumer;

import bao.study.mymq.common.ServiceThread;

/**
 * @author baoyh
 * @since 2022/10/28 14:31
 */
public class PullMessageService extends ServiceThread {

    @Override
    public void run() {
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }
}
