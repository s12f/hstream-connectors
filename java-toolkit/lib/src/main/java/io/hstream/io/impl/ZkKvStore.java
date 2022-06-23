package io.hstream.io.impl;

import io.hstream.io.KvStore;
import java.io.IOException;
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
    public void set(String key, byte[] val) throws InterruptedException, KeeperException {
        var path = kvPath + "/" + key;
        if (zk.exists(path, false) == null) {
            try {
                zk.create(path, val, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                return;
            } catch (KeeperException.NodeExistsException ignored) {}
        }
        zk.setData(path, val, -1);
    }

    @Override
    public byte[] get(String key) throws Exception {
        var path = kvPath + "/" + key;
        var stat = new Stat();
        try {
            return zk.getData(path, false, stat);
        } catch (KeeperException.NoNodeException e) {
            return null;
        }
    }

    @Override
    public void close() throws InterruptedException {
        zk.close();
    }
}
