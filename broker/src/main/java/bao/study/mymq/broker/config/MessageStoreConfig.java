package bao.study.mymq.broker.config;

import bao.study.mymq.common.Constant;

import java.io.File;

/**
 * @author baoyh
 * @since 2022/8/31 16:43
 */
public class MessageStoreConfig {

    private String rootPath;

    private String commitLogPath;

    private long commitLogFileSize = 1024 * 1024;

    private String consumerQueuePath;

    public String getRootPath() {
        String homePath = System.getProperty(Constant.MYMQ_HOME_PROPERTY, System.getenv(Constant.MYMQ_HOME_ENV));
        rootPath = homePath == null ? rootPath : homePath + File.separator + "store";
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    public String getCommitLogPath() {
        if (commitLogPath == null) {
            commitLogPath = getRootPath() + File.separator + "commitlog";
        }
        return commitLogPath;
    }

    public void setCommitLogPath(String commitLogPath) {
        this.commitLogPath = commitLogPath;
    }

    public String getConsumerQueuePath() {
        if (consumerQueuePath == null) {
            consumerQueuePath = getRootPath() + File.separator + "consumerqueue";
        }
        return consumerQueuePath;
    }

    public void setConsumerQueuePath(String consumerQueuePath) {
        this.consumerQueuePath = consumerQueuePath;
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
