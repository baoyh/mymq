package bao.study.mymq.broker.util;

import bao.study.mymq.broker.BrokerException;
import bao.study.mymq.broker.store.MappedFile;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author baoyh
 * @since 2023/6/1 17:35
 */
public class MappedFileHelper {

    public static MappedFile latestMappedFile(List<MappedFile> mappedFileList) {
        if (mappedFileList.isEmpty()) throw new BrokerException("MappedFile cannot empty");
        return mappedFileList.get(mappedFileList.size() - 1);
    }

    public static MappedFile find(long offset, List<MappedFile> mappedFileList) {
        for (MappedFile mappedFile : mappedFileList) {
            if (mappedFile.getFileFromOffset() <= offset && mappedFile.getFileFromOffset() + mappedFile.getFileSize() >= offset) {
                return mappedFile;
            }
        }
        throw new BrokerException("Cannot find the mapped file by offset [" + offset + "]");
    }

    public static List<MappedFile> load(final String fileName, final long fileSize) {
        List<MappedFile> list = new ArrayList<>();
        File file = new File(fileName);
        if (file.exists()) {
            Arrays.stream(file.listFiles()).forEach(f -> list.add(new MappedFile(f.getPath(), fileSize)));
        }
        return list;
    }
}
