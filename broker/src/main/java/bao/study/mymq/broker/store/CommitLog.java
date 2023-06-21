package bao.study.mymq.broker.store;

import bao.study.mymq.broker.config.MessageStoreConfig;
import bao.study.mymq.broker.util.MappedFileHelper;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author baoyh
 * @since 2022/8/26 14:00
 */
public class CommitLog {

    private final MessageStoreConfig messageStoreConfig;

    private List<MappedFile> mappedFileList = new CopyOnWriteArrayList<>();

    public CommitLog(MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
    }

    public MappedFile latestMappedFile() {
        return MappedFileHelper.latestMappedFile(mappedFileList);
    }

    public ConsumeQueueOffset appendMessage(MessageStore messageStore) {
        MappedFile mappedFile = latestMappedFile();
        return mappedFile.appendMessage(messageStore);
    }

    public MessageStore read(long offset, int size) {
        MappedFile mappedFile = MappedFileHelper.find(offset, mappedFileList);
        return MessageStoreCodec.decode(mappedFile.read((int) (offset - mappedFile.getFileFromOffset()), size));
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public void setMappedFileList(List<MappedFile> mappedFileList) {
        this.mappedFileList = mappedFileList;
    }
}
