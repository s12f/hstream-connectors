package io.hstream.io;

import io.hstream.HRecord;
import io.hstream.io.KvStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileKvStore implements KvStore {
    Path filePath;

    public FileKvStore(String filePath) {
        this.filePath = Paths.get(filePath);
    }

    @Override
    public String get(String key) throws Exception {
        synchronized (this) {
            String dataText = Files.readString(filePath);
            var data = HRecord.newBuilder().merge(dataText).build();
            if (data.contains(key)) {
                return data.getString(key);
            }
            return null;
        }
    }

    @Override
    public void set(String key, String val) throws Exception {
        synchronized (this) {
            String dataText = Files.readString(filePath);
            var newData = HRecord.newBuilder().merge(dataText).put(key, val).build();
            Files.writeString(filePath, newData.toJsonString());
        }
    }
}