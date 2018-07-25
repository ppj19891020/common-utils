package com.fly.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class ZKUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKUtils.class);

    static CuratorFramework curatorFramework = null;
    static ConcurrentMap<String, PathChildrenCache> watchMap = new ConcurrentHashMap<>();
    static {
        curatorFramework = createConnection();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                if (curatorFramework != null)
                    curatorFramework.close();
                watchMap.clear();
            }
        }));
    }

    public static CuratorFramework getConnection() {
        return curatorFramework;
    }

    private static CuratorFramework createConnection() {
        String url = "zk1.host.dxy:2181";
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(url, new ExponentialBackoffRetry(100, 6));
        // start connection
        curatorFramework.start();
        // wait 3 second to establish connect
        try {
            curatorFramework.blockUntilConnected(3, TimeUnit.SECONDS);
            if (curatorFramework.getZookeeperClient().isConnected()) {
                return curatorFramework;
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }

        // fail situation
        curatorFramework.close();
        throw new RuntimeException("failed to connect to zookeeper service : " + url);
    }

    /**
     * 获取子节点信息
     * @param client
     * @param path
     * @return
     */
    public static List<String> getChildByPath(CuratorFramework client,String path){
        try {
            Stat stat = client.checkExists().forPath(path);
            if(stat!=null){
                List<String> childNodes = client.getChildren().forPath(path);
                return childNodes;
            }
        }catch (Exception ex){
            LOGGER.error("获取子节点失败",ex);
            return Collections.emptyList();
        }
        return null;
    }

}
