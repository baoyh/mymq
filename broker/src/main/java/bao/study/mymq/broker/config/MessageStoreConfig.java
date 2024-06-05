package bao.study.mymq.broker.config;

import java.io.File;

/**
 * @author baoyh
 * @since 2022/8/31 16:43
 */
public class MessageStoreConfig extends BrokerConfig {

    private String commitLogPath;

    private long commitLogFileSize = 1024 * 1024;

    public String getCommitLogPath() {
        if (commitLogPath == null) {
            commitLogPath = dataPath() + File.separator + "commitlog";
        }
        return commitLogPath;
    }

    public String getCommitLogFirstName() {
        return "00000000";
    }

    public long getCommitLogFileSize() {
        return commitLogFileSize;
    }

    public void setCommitLogFileSize(int commitLogFileSize) {
        this.commitLogFileSize = commitLogFileSize;
    }
}
