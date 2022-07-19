package io.hstream.io.impl;

import io.hstream.io.KvStore;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZkKvStore implements KvStore {
    ZooKeeper zk;
    String kvPath;

    ZkKvStore(String url, String kvPath) throws Exception {
        this.zk = new ZooKeeper(url, 100, event -> {});
        this.kvPath = kvPath;
        if (zk.exists(kvPath, false) == null) {
            zk.create(kvPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    @Override
    public void set(String key, String val) {
        var path = kvPath + "/" + key;
        try {
            if (zk.exists(path, false) == null) {
                try {
                    zk.create(path, val.getBytes(StandardCharsets.UTF_8),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    return;
                } catch (KeeperException.NodeExistsException ignored) {}
            }
            zk.setData(path, val.getBytes(StandardCharsets.UTF_8), -1);
        } catch (KeeperException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String get(String key) {
        var path = kvPath + "/" + key;
        var stat = new Stat();
        try {
            return new String(zk.getData(path, false, stat), StandardCharsets.UTF_8);
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    // TODO: atomically read
    @Override
    public Map<String, String> toMap() {
        var result = new HashMap<String, String>();
        try {
            var keys = zk.getChildren(kvPath, false);
            for (var key : keys) {
                var val = zk.getData(kvPath + "/" + key, false, new Stat());
                result.put(key, new String(val, StandardCharsets.UTF_8));
            }
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public void close() throws InterruptedException {
        zk.close();
    }
}
