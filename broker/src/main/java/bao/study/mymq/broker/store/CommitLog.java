package bao.study.mymq.broker.store;

import bao.study.mymq.broker.config.MessageStoreConfig;

import java.io.File;
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
        if (mappedFileList.isEmpty()) {
            mappedFileList.add(new MappedFile(
                    messageStoreConfig.getCommitLogPath() + File.separator + messageStoreConfig.getCommitLogFirstName(),
                    messageStoreConfig.getCommitLogFileSize()));
        }
        return mappedFileList.get(mappedFileList.size() - 1);
    }

    public void appendMessage(MessageStore messageStore) {
        MappedFile mappedFile = latestMappedFile();
        mappedFile.appendMessage(messageStore);
    }

    public void read() {
        MappedFile mappedFile = latestMappedFile();
        MessageStore messageStore = MessageStoreCodec.decode(mappedFile.read(0, 74));
        System.out.println(messageStore);
    }

}
