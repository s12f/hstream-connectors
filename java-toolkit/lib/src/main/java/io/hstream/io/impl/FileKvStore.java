package io.hstream.io.impl;

import ch.qos.logback.core.encoder.EchoEncoder;
import io.hstream.HRecord;
import io.hstream.io.KvStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Map;
import java.util.stream.Collectors;

public class FileKvStore implements KvStore {
    Path filePath;

    public FileKvStore(String filePath) {
        this.filePath = Paths.get(filePath);
    }

    @Override
    public String get(String key) {
        synchronized (this) {
            String dataText = null;
            try {
                dataText = Files.readString(filePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            var data = HRecord.newBuilder().merge(dataText).build();
            if (data.contains(key)) {
                return data.getString(key);
            }
            return null;
        }
    }

    @Override
    public Map<String, String> toMap() {
        synchronized (this) {
            try {
                String dataText = Files.readString(filePath);
                var data = HRecord.newBuilder().merge(dataText).build();
                return data.getKeySet().stream().collect(Collectors.toMap(k -> k, data::getString));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void set(String key, String val) {
        synchronized (this) {
            String dataText = null;
            try {
                dataText = Files.readString(filePath);
                var newData = HRecord.newBuilder().merge(dataText).put(key, val).build();
                Files.writeString(filePath, newData.toJsonString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() {}

}