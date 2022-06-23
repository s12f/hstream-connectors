package io.hstream.io.impl;

import io.hstream.HRecord;
import io.hstream.io.KvStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;

public class FileKvStore implements KvStore {
    Path filePath;

    public FileKvStore(String filePath) {
        this.filePath = Paths.get(filePath);
    }

    @Override
    public byte[] get(String key) throws Exception {
        synchronized (this) {
            String dataText = Files.readString(filePath);
            var data = HRecord.newBuilder().merge(dataText).build();
            if (data.contains(key)) {
                return Base64.getDecoder().decode(data.getString(key));
            }
            return null;
        }
    }

    @Override
    public void set(String key, byte[] val) throws Exception {
        synchronized (this) {
            String dataText = Files.readString(filePath);
            var newData = HRecord.newBuilder().merge(dataText).put(key, Base64.getEncoder().encodeToString(val)).build();
            Files.writeString(filePath, newData.toJsonString());
        }
    }

    @Override
    public void close() {}

}