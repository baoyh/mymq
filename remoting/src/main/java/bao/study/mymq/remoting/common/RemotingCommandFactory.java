package bao.study.mymq.remoting.common;

/**
 * @author baoyh
 * @since 2022/8/1 17:07
 */
public abstract class RemotingCommandFactory {

    public static RemotingCommand createRequestRemotingCommand(int code, byte[] body) {
        RemotingCommand remotingCommand = createRemotingCommand(code, body);
        remotingCommand.setRemotingCommandType(RemotingCommandType.REQUEST);
        return remotingCommand;
    }

    public static RemotingCommand createResponseRemotingCommand(int code, byte[] body) {
        RemotingCommand remotingCommand = createRemotingCommand(code, body);
        remotingCommand.setRemotingCommandType(RemotingCommandType.RESPONSE);
        return remotingCommand;
    }

    private static RemotingCommand createRemotingCommand(int code, byte[] body) {
        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setBody(body);
        remotingCommand.setCode(code);
        return remotingCommand;
    }
}
