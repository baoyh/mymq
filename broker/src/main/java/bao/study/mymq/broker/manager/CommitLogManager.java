package bao.study.mymq.broker.manager;

import bao.study.mymq.broker.BrokerProperties;
import bao.study.mymq.broker.config.BrokerConfig;
import bao.study.mymq.broker.config.MessageStoreConfig;
import bao.study.mymq.broker.store.CommitLog;
import bao.study.mymq.broker.store.MappedFile;
import bao.study.mymq.common.utils.CommonCodec;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author baoyh
 * @since 2023/6/8 16:31
 */
public class CommitLogManager extends ConfigManager {

    private final transient CommitLog commitLog;

    private final transient BrokerProperties brokerProperties;

    private ConcurrentMap<String/* mappedFile name */, Integer/* committedPosition */> committedTable = new ConcurrentHashMap<>();

    public CommitLogManager(CommitLog commitLog, BrokerProperties brokerProperties) {
        this.commitLog = commitLog;
        this.brokerProperties = brokerProperties;
    }

    public void updateCommittedTable() {
        MappedFile mappedFile = commitLog.latestMappedFile();
        committedTable.put(mappedFile.getFile().getName(), mappedFile.getCommittedPosition().get());

        // TODO asynchronous
        commit();
    }

    @Override
    public void load() {
        super.load();
        MessageStoreConfig messageStoreConfig = commitLog.getMessageStoreConfig();
        File folder = new File(messageStoreConfig.getCommitLogPath());
        List<MappedFile> mappedFileList = commitLog.getMappedFileList();

        if (!folder.exists() || folder.listFiles().length == 0) {
            mappedFileList.add(new MappedFile(
                    messageStoreConfig.getCommitLogPath() + File.separator + messageStoreConfig.getCommitLogFirstName(),
                    messageStoreConfig.getCommitLogFileSize()));
        } else {
            for (File file : folder.listFiles()) {
                Integer committedIndex = committedTable.get(file.getName());
                if (committedIndex == null) {
                    committedIndex = 0;
                }
                mappedFileList.add(new MappedFile(file.getPath(), messageStoreConfig.getCommitLogFileSize(), committedIndex));
            }
        }
    }

    @Override
    public String encode() {
        return CommonCodec.encode2String(this);
    }

    @Override
    public void decode(String json) {
        if (json == null || json.isEmpty()) return;
        CommitLogManager commitLogManager = CommonCodec.decode(json.getBytes(StandardCharsets.UTF_8), CommitLogManager.class);
        this.committedTable = commitLogManager.getCommittedTable();
    }

    @Override
    public String configFilePath() {
        return BrokerConfig.getConfigRootPath() + File.separator + brokerProperties.getBrokerName() +
                File.separator + brokerProperties.getBrokerId() + File.separator + BrokerConfig.commitlogConfigName();
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    public ConcurrentMap<String, Integer> getCommittedTable() {
        return committedTable;
    }
}
