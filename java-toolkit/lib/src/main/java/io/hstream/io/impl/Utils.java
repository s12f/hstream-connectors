package io.hstream.io.impl;

import io.hstream.HRecord;
import io.hstream.io.KvStore;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class Utils {
    static KvStore makeKvStoreFromConfig(HRecord cfg) {
        var kvCfg = cfg.getHRecord("kv");
        var kvType = kvCfg.getString("type");
        if (kvType.equals("zk")) {
            try {
                return new ZkKvStore(kvCfg.getString("url"), kvCfg.getString("rootPath"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else if (kvType.equals("file")) {
            return new FileKvStore(kvCfg.getString("filePath"));
        } else {
            throw new RuntimeException("can't handle kv.type:" + kvType);
        }
    }

    public static Map<String, String> parseStreams(String s) {
        return Arrays.stream(s.split(";"))
                .map(p -> getStreamPair(s))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map.Entry<String, String> getStreamPair(String s) {
        var xs = s.split(":");
        if (xs.length == 1) {
            return Map.entry(s, s);
        }
        if (xs.length == 2) {
            return Map.entry(xs[0], xs[1]);
        }
        throw new RuntimeException("invalid stream:" + s);
    }
}
