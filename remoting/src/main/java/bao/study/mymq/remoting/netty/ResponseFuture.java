package bao.study.mymq.remoting.netty;

import bao.study.mymq.remoting.InvokeCallback;
import bao.study.mymq.remoting.RemotingException;
import bao.study.mymq.remoting.common.RemotingCommand;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author baoyh
 * @since 2022/8/12 17:56
 */
public class ResponseFuture {

    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    private volatile RemotingCommand responseCommand;

    private volatile InvokeCallback invokeCallback;

    private volatile RuntimeException exception;

    public RemotingCommand awaitResponse(long time) {
        try {
            countDownLatch.await(time, TimeUnit.MILLISECONDS);
            return responseCommand;
        } catch (InterruptedException e) {
            throw new RemotingException("await response interrupted", e);
        }
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }

    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }

    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }

    public void setInvokeCallback(InvokeCallback invokeCallback) {
        this.invokeCallback = invokeCallback;
    }

    public RuntimeException getException() {
        return exception;
    }

    public void setException(RuntimeException exception) {
        this.exception = exception;
    }
}
