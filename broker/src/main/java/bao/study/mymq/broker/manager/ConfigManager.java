package bao.study.mymq.broker.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author baoyh
 * @since 2022/10/25 16:42
 */
public abstract class ConfigManager {

    private static final Logger log = LoggerFactory.getLogger(ConfigManager.class);

    public boolean load() {
        String filePath = configFilePath();
        File file = new File(filePath);
        try {
            String fileStr = file2String(file);
            if (fileStr != null) {
                decode(fileStr);
                return true;
            }
        } catch (Exception e) {
            log.error("load file " + file.getName() + " fail");
        }
        return false;
    }

    public abstract String encode();

    public abstract void decode(String json);

    public abstract String configFilePath();

    private String file2String(File file) throws IOException {
        try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            byte[] buffer = new byte[(int) file.length()];
            int read = inputStream.read(buffer);
            if (read == (int) file.length()) {
                return new String(buffer, StandardCharsets.UTF_8);
            }
            return null;
        }
    }
}
