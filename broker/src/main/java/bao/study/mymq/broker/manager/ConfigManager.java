package bao.study.mymq.broker.manager;

import bao.study.mymq.broker.BrokerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * @author baoyh
 * @since 2022/10/25 16:42
 */
public abstract class ConfigManager {

    private static final Logger log = LoggerFactory.getLogger(ConfigManager.class);

    public boolean load() {
        File file = new File(configFilePath());
        try {
            String fileStr = file2String(file);
            if (fileStr != null) {
                decode(fileStr);
                return true;
            }
        } catch (Exception e) {
            log.error("Load file " + file.getName() + " fail", e);
        }
        return false;
    }

    public void commit() {
        RandomAccessFile file;
        try {
            file = new RandomAccessFile(configFilePath(), "rw");
            file.setLength(0);
            file.write(encode().getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("Commit file fail", e);
        }
    }

    public String encode() {
        throw new BrokerException("Please rewrite the method");
    }

    public void decode(String json) {
        throw new BrokerException("Please rewrite the method");
    }

    public abstract String configFilePath();

    private String file2String(File file) throws IOException {
        try (BufferedInputStream inputStream = new BufferedInputStream(Files.newInputStream(file.toPath()))) {
            byte[] buffer = new byte[(int) file.length()];
            int read = inputStream.read(buffer);
            if (read == (int) file.length()) {
                return new String(buffer, StandardCharsets.UTF_8);
            }
            return null;
        }
    }
}
