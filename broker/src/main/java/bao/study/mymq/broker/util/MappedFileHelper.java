package bao.study.mymq.broker.util;

import bao.study.mymq.broker.BrokerException;
import bao.study.mymq.broker.store.MappedFile;

import java.util.List;

/**
 * @author baoyh
 * @since 2023/6/1 17:35
 */
public class MappedFileHelper {

    public static MappedFile find(long offset, List<MappedFile> mappedFileList) {
        for (int i = 0; i < mappedFileList.size(); i++) {
            if (mappedFileList.get(i).fileFromOffset > offset) {
                return mappedFileList.get(Math.max((i - 1), 0));
            }
        }
        throw new BrokerException("Cannot find the mapped file by offset [" + offset + "]");
    }
}
