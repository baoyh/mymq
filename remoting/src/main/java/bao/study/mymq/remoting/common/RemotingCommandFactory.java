package bao.study.mymq.remoting.common;

/**
 * @author baoyh
 * @since 2022/8/1 17:07
 */
public abstract class RemotingCommandFactory {

    public static RemotingCommand createRemotingCommand(int code, byte[] body) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setBody(body);
        remotingCommand.setCode(code);
        return remotingCommand;
    }
}
