package bao.study.mymq.broker.raft.store;

import bao.study.mymq.broker.raft.Config;
import bao.study.mymq.broker.store.MappedFile;
import bao.study.mymq.broker.util.MappedFileHelper;
import bao.study.mymq.common.protocol.raft.RaftEntry;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
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

    private Config config;

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
     * 已提交的日志索引
     */
    private volatile long committedIndex = -1;
    /**
     * 已提交的日志偏移量
     */
    private volatile long committedPos = -1;
    /**
     * 当前最大的投票轮次
     */
    private volatile long endTerm;

    /**
     * 存放 entry 数据
     */
    private List<MappedFile> dataFileList = new CopyOnWriteArrayList<>();

    /**
     * 存放 entry 的索引, 被设计成定长, 区别于 RaftEntry(因为存在 body 所以不定长,也就无法快速访问)
     * 可以根据 index 快速定位到 dataFile 中的偏移量, 方便拿到 entry
     */
    private List<MappedFile> indexFileList = new CopyOnWriteArrayList<>();

    public RaftFileStore(Config config) {
        this.config = config;
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
        this.committedIndex = committedIndex;
        this.committedPos = committedIndex * INDEX_UNIT_SIZE;
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

}
