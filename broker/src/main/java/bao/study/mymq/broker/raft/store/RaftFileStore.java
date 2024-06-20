package bao.study.mymq.broker.raft.store;

import bao.study.mymq.broker.raft.Config;
import bao.study.mymq.broker.store.MappedFile;
import bao.study.mymq.broker.util.MappedFileHelper;
import bao.study.mymq.common.ServiceThread;
import bao.study.mymq.common.protocol.raft.RaftEntry;
import bao.study.mymq.common.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static bao.study.mymq.broker.raft.store.RaftEntryIndex.INDEX_UNIT_SIZE;

/**
 * TODO: 单文件满了之后的处理, 过期数据定期删除
 *
 * @author baoyh
 * @since 2024/5/21 16:30
 */
public class RaftFileStore extends RaftStore {

    private static final Logger logger = LoggerFactory.getLogger(RaftFileStore.class);

    public static final String CHECK_POINT_FILE = "checkpoint";
    public static final String END_INDEX_KEY = "endIndex";
    public static final String COMMITTED_INDEX_KEY = "committedIndex";

    private final Config config;

    /**
     * 日志的起始索引
     */
    private volatile long beginIndex = -1;
    /**
     * 下一条日志下标
     */
    private volatile long endIndex = -1;

    private final AtomicInteger wrotePosition = new AtomicInteger(0);

    /**
     * 已提交的日志索引, 表示在这之前的日志都已经完成了同步, 同时用于节点宕机重启后的日志进度标识
     */
    private volatile long committedIndex = -1;

    /**
     * 当前最大的投票轮次
     */
    private volatile long endTerm;

    /**
     * 存放 entry 数据
     */
    private final List<MappedFile> dataFileList = new CopyOnWriteArrayList<>();

    /**
     * 存放 entry 的索引, 被设计成定长, 区别于 RaftEntry(因为存在 body 所以不定长,也就无法快速访问)
     * 可以根据 index 快速定位到 dataFile 中的偏移量, 方便拿到 entry
     */
    private final List<MappedFile> indexFileList = new CopyOnWriteArrayList<>();

    /**
     * 持久化 committedIndex、endIndex
     */
    private final FlushService flushService = new FlushService();

    public RaftFileStore(Config config) {
        this.config = config;
    }

    @Override
    public void startup() {
        load();
        recover();
        flushService.start();
    }

    private void load() {
        dataFileList.addAll(MappedFileHelper.load(config.getDataStorePath(), config.getDataFileSize()));
        indexFileList.addAll(MappedFileHelper.load(config.getIndexStorePath(), config.getIndexFileSize()));
    }

    private void recover() {
        Properties properties = loadCheckPoint();
        if (properties == null) {
            return;
        }

        endIndex = Long.parseLong(properties.getProperty(END_INDEX_KEY));
        committedIndex = Long.parseLong(properties.getProperty(COMMITTED_INDEX_KEY));

        MappedFile indexFile = MappedFileHelper.latestMappedFile(indexFileList);
        ByteBuffer dataBuffer = indexFile.read((int) committedIndex * INDEX_UNIT_SIZE, INDEX_UNIT_SIZE);
        RaftEntryIndex entryIndex = RaftEntryIndexCodec.decode(dataBuffer);
        if (entryIndex == null) {
            return;
        }
        indexFile.setWrotePosition((int) committedIndex * INDEX_UNIT_SIZE + INDEX_UNIT_SIZE);
        indexFile.setCommittedPosition((int) committedIndex * INDEX_UNIT_SIZE + INDEX_UNIT_SIZE);

        MappedFile dataFile = MappedFileHelper.latestMappedFile(dataFileList);
        int committedPos = (int) entryIndex.getPos() + entryIndex.getSize();
        dataFile.setWrotePosition(committedPos);
        dataFile.setCommittedPosition(committedPos);
        wrotePosition.set(committedPos);
    }


    @Override
    public void shutdown() {
        flushService.shutdown();
    }

    @Override
    public synchronized RaftEntry append(RaftEntry entry) {
        entry.computeSizeInBytes();
        entry.setIndex(endIndex + 1);
        entry.setTerm(entry.getTerm());
        entry.setPos(getPos());

        appendDataFile(entry);
        appendIndexFile(entry);

        endIndex++;
        wrotePosition.addAndGet(entry.getSize());
        if (beginIndex == -1) {
            beginIndex = endIndex;
        }

        return entry;
    }

    @Override
    public RaftEntry get(long index) {
        RaftEntryIndex entryIndex = getIndex(index);
        MappedFile dataFile = getLastDataFile();
        ByteBuffer dataBuffer = dataFile.read((int) entryIndex.getPos(), entryIndex.getSize());
        return RaftEntryCodec.decode(dataBuffer);
    }

    private RaftEntryIndex getIndex(long index) {
        MappedFile indexFile = getLastIndexFile();
        ByteBuffer indexBuffer = indexFile.read((int) index * INDEX_UNIT_SIZE, INDEX_UNIT_SIZE);
        return RaftEntryIndexCodec.decode(indexBuffer);
    }

    @Override
    public long getCommittedIndex() {
        return committedIndex;
    }

    @Override
    public void updateCommittedIndex(long term, long committedIndex) {
        logger.debug("update CommittedIndex, term={}, committedIndex={}", term, committedIndex);
        this.committedIndex = committedIndex;
        this.endTerm = term;
        flushService.wakeup();
    }

    @Override
    public long getEndTerm() {
        return endTerm;
    }

    @Override
    public long getEndIndex() {
        return endIndex;
    }

    @Override
    public long getBeginIndex() {
        return beginIndex;
    }

    private void appendDataFile(RaftEntry entry) {
        MappedFile dataFile = getLastDataFile();
        ByteBuffer buffer = RaftEntryCodec.encode(entry);
        buffer.flip();
        dataFile.append(buffer);
    }

    private void appendIndexFile(RaftEntry entry) {
        RaftEntryIndex entryIndex = new RaftEntryIndex();
        entryIndex.setSize(entry.getSize());
        entryIndex.setTerm(entry.getTerm());
        entryIndex.setPos(entry.getPos());
        entryIndex.setIndex(entry.getIndex());

        MappedFile indexFile = getLastIndexFile();
        ByteBuffer buffer = RaftEntryIndexCodec.encode(entryIndex);
        buffer.flip();
        indexFile.append(buffer);
    }

    private long getPos() {
        if (endIndex == -1) return 0;
        return wrotePosition.get();
    }

    private MappedFile getLastDataFile() {
        return dataFileList.isEmpty() ? createDataFile() : MappedFileHelper.latestMappedFile(dataFileList);
    }

    public List<MappedFile> getDataFileList() {
        return dataFileList;
    }

    private MappedFile createDataFile() {
        String dataStorePath = config.getDataStorePath();
        MappedFile mappedFile = new MappedFile(dataStorePath + File.separator + "00000000", config.getDataFileSize());
        dataFileList.add(mappedFile);
        return mappedFile;
    }

    private MappedFile getLastIndexFile() {
        return indexFileList.isEmpty() ? createIndexFile() : MappedFileHelper.latestMappedFile(indexFileList);
    }

    private MappedFile createIndexFile() {
        String indexStorePath = config.getIndexStorePath();
        MappedFile mappedFile = new MappedFile(indexStorePath + File.separator + "0000000000", config.getIndexFileSize());
        indexFileList.add(mappedFile);
        return mappedFile;
    }

    private void persistCheckPoint() {
        try {
            Properties properties = new Properties();
            properties.put(END_INDEX_KEY, getEndIndex());
            properties.put(COMMITTED_INDEX_KEY, getCommittedIndex());
            String data = IOUtils.properties2String(properties);
            IOUtils.string2File(data, config.getDefaultPath() + File.separator + CHECK_POINT_FILE);
        } catch (Throwable t) {
            logger.error("Persist checkpoint failed", t);
        }
    }

    private Properties loadCheckPoint() {
        try {
            String data = IOUtils.file2String(config.getDefaultPath() + File.separator + CHECK_POINT_FILE);
            return IOUtils.string2Properties(data);
        } catch (Throwable t) {
            logger.error("Load checkpoint failed", t);
            return null;
        }
    }

    private class FlushService extends ServiceThread {

        @Override
        public String getServiceName() {
            return FlushService.class.getSimpleName();
        }

        @Override
        public void run() {
            while (!stop) {
                waitForWakeup();
                persistCheckPoint();
                reset();
            }
        }

        @Override
        public void shutdown() {
            super.shutdown();
//            persistCheckPoint();
        }
    }
}
