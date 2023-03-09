package bao.study.mymq.broker.store;

import bao.study.mymq.broker.BrokerException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author baoyh
 * @since 2022/7/15 14:24
 */
public class MappedFile {

    /**
     * 当前文件的写指针
     */
    private final AtomicInteger wrotePosition = new AtomicInteger(0);

    /**
     * 当前文件的提交指针
     */
    private final AtomicInteger committedPosition = new AtomicInteger(0);

    /**
     * 已经完成刷盘的位置
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    /**
     * 文件名
     */
    private final String fileName;

    /**
     * 文件大小
     */
    private final int fileSize;

    /**
     * 文件初始偏移量
     */
    private final long fileFromOffset;

    /**
     * 物理文件
     */
    private final File file;

    /**
     * 数据会先放入 writeBuffer, 再之后刷盘时放入 mappedByteBuffer
     */
    private ByteBuffer writeBuffer;

    /**
     * 文件通道
     */
    private FileChannel fileChannel;

    /**
     * 物理文件对应的内存映射 buffer
     */
    private MappedByteBuffer mappedByteBuffer;

    /**
     * 文件最后一次写入时间
     */
    private volatile long storeTimestamp = 0;

    public MappedFile(final String fileName, final int fileSize) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        fileFromOffset = Long.parseLong(file.getName());

        try {
            if (!file.exists()) {
                file.getParentFile().mkdir();
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void appendMessage(MessageStore messageStore) {
        ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : mappedByteBuffer.slice();
        byteBuffer.position(wrotePosition.get());

        messageStore.setCommitLogOffset(wrotePosition.get());
        ByteBuffer messageBuffer = MessageStoreCodec.encode(messageStore);
        messageBuffer.flip();
        byteBuffer.put(messageBuffer);

        messageBuffer.flip();
        int size = messageBuffer.getInt();
        wrotePosition.addAndGet(size);

        commit();
    }

    public void commit() {
        int wrotePos = wrotePosition.get();
        int committedPos = committedPosition.get();

        if (committedPos >= wrotePos) {
            return;
        }

        ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : mappedByteBuffer.slice();
        byteBuffer.position(committedPos);
        byteBuffer.limit(wrotePos);

        try {
            fileChannel.position(committedPos);
            fileChannel.write(byteBuffer);
            committedPosition.set(wrotePos);

            storeTimestamp = file.lastModified();
        } catch (IOException e) {
            throw new BrokerException(e);
        }
    }

    public void read() {
        ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : mappedByteBuffer.slice();
        byteBuffer.position(0);
        byteBuffer.limit(74);

        MessageStore messageStore = MessageStoreCodec.decode(byteBuffer);
        System.out.println(messageStore);
    }
}
