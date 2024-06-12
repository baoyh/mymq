package bao.study.mymq.broker.store;

import bao.study.mymq.broker.BrokerException;
import bao.study.mymq.broker.config.MessageStoreConfig;
import bao.study.mymq.broker.raft.RaftServer;
import bao.study.mymq.broker.raft.store.RaftEntryCodec;
import bao.study.mymq.broker.util.MappedFileHelper;
import bao.study.mymq.common.protocol.raft.AppendEntryRequest;
import bao.study.mymq.common.protocol.raft.AppendEntryResponse;
import bao.study.mymq.common.protocol.raft.RaftEntry;
import bao.study.mymq.remoting.code.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author baoyh
 * @since 2024/6/5 15:30
 */
public class RaftCommitLog extends CommitLog {

    private static final Logger logger = LoggerFactory.getLogger(RaftCommitLog.class);

    private final RaftServer raftServer;

    public RaftCommitLog(MessageStoreConfig messageStoreConfig, RaftServer raftServer) {
        super(messageStoreConfig, raftServer.getEntryProcessor().getDataFileList());
        this.raftServer = raftServer;
        messageStoreConfig.setCommitLogFileSize(raftServer.getConfig().getDataFileSize());
        messageStoreConfig.setCommitLogPath(raftServer.getConfig().getDataStorePath());
    }

    @Override
    public ConsumeQueueOffset appendMessage(MessageStore messageStore) {
        AppendEntryRequest entryRequest = new AppendEntryRequest();
        ByteBuffer buffer = MessageStoreCodec.encode(messageStore);
        int size = RaftEntry.HEADER_SIZE + buffer.limit();
        entryRequest.setBody(buffer.array());
        CompletableFuture<AppendEntryResponse> appendFuture = raftServer.getEntryProcessor().handleAppend(entryRequest);
        try {
            AppendEntryResponse response = appendFuture.get(raftServer.getConfig().getRpcTimeoutMillis(), TimeUnit.MILLISECONDS);
            if (response.getCode() != ResponseCode.SUCCESS) {
                throw new BrokerException("Append message fail, topic is " + messageStore.getTopic());
            }
            return new ConsumeQueueOffset(response.getPos(), size);
        } catch (Throwable ex) {
            throw new BrokerException(ex.getMessage());
        }
    }

    @Override
    public MessageStore read(long offset, int size) {
        MappedFile mappedFile = MappedFileHelper.find(offset, mappedFileList);
        RaftEntry entry = RaftEntryCodec.decode(mappedFile.read((int) (offset - mappedFile.getFileFromOffset()), size));
        if (entry == null) {
            throw new BrokerException(String.format("Read message fail, offset is %d, size is %d", offset, size));
        }
        ByteBuffer message = ByteBuffer.allocate(entry.getBody().length).put(entry.getBody());
        message.flip();
        return MessageStoreCodec.decode(message);
    }
}
