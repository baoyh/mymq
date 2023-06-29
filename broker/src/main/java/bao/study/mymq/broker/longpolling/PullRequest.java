package bao.study.mymq.broker.longpolling;

import bao.study.mymq.remoting.common.RemotingCommand;
import io.netty.channel.Channel;


/**
 * @author baoyh
 * @since 2023/6/28 16:54
 */
public class PullRequest {

    private final RemotingCommand remotingCommand;

    private final Channel channel;

    private final int requestId;

    private final long holdTime;

    private final long beginTime;

    public PullRequest(RemotingCommand remotingCommand, Channel channel, int requestId, long holdTime, long beginTime) {
        this.remotingCommand = remotingCommand;
        this.channel = channel;
        this.requestId = requestId;
        this.holdTime = holdTime;
        this.beginTime = beginTime;
    }

    public RemotingCommand getRemotingCommand() {
        return remotingCommand;
    }

    public long getHoldTime() {
        return holdTime;
    }

    public Channel getChannel() {
        return channel;
    }

    public long getBeginTime() {
        return beginTime;
    }

    public int getRequestId() {
        return requestId;
    }
}
