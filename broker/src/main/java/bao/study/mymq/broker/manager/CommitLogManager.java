package bao.study.mymq.broker.manager;

import bao.study.mymq.broker.config.BrokerConfig;
import bao.study.mymq.broker.config.MessageStoreConfig;
import bao.study.mymq.broker.store.CommitLog;
import bao.study.mymq.broker.store.MappedFile;
import bao.study.mymq.common.utils.CommonCodec;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author baoyh
 * @since 2023/6/8 16:31
 */
public class CommitLogManager extends ConfigManager {

    private final transient CommitLog commitLog;

    private ConcurrentMap<String/* mappedFile name */, Integer/* committedPosition */> committedTable;

    public CommitLogManager(CommitLog commitLog) {
        this.commitLog = commitLog;
    }

    public void updateCommittedTable() {
        MappedFile mappedFile = commitLog.latestMappedFile();
        committedTable.put(mappedFile.getFile().getName(), mappedFile.getCommittedPosition().get());

        // TODO asynchronous
        commit();
    }

    @Override
    public boolean load() {
        super.load();

        MessageStoreConfig messageStoreConfig = commitLog.getMessageStoreConfig();
        File folder = new File(messageStoreConfig.getCommitLogPath());
        File[] files = folder.listFiles();
        assert files != null;

        List<MappedFile> mappedFileList = new CopyOnWriteArrayList<>();
        if (files.length == 0) {
            mappedFileList.add(new MappedFile(
                    messageStoreConfig.getCommitLogPath() + File.separator + messageStoreConfig.getCommitLogFirstName(),
                    messageStoreConfig.getCommitLogFileSize()));
        } else {
            for (File file : files) {
                mappedFileList.add(new MappedFile(file.getPath(), messageStoreConfig.getCommitLogFileSize(), committedTable.get(file.getName())));
            }
        }
        commitLog.setMappedFileList(mappedFileList);
        return true;
    }

    @Override
    public String encode() {
        return CommonCodec.encode2String(this);
    }

    @Override
    public void decode(String json) {
        CommitLogManager commitLogManager = CommonCodec.decode(json.getBytes(StandardCharsets.UTF_8), CommitLogManager.class);
        this.committedTable = commitLogManager.getCommittedTable();
    }

    @Override
    public String configFilePath() {
        return BrokerConfig.commitlogConfigPath();
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    public ConcurrentMap<String, Integer> getCommittedTable() {
        return committedTable;
    }
}
