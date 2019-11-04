package com.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs;

import java.util.List;

public class TestZookeeper {
    private static String connectString = "hadoop.spark.com:2181";
    private static int sessionTimeout = 2000;
    //private static ZooKeeper zkClient = null;

    public static void main(String[] args) throws Exception {
        ZooKeeper  zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
          public void process(WatchedEvent event) {
                // 收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println(event.getType() + "--" + event.getPath());

            }

        });
        //create(zkClient);
        getChildren(zkClient);
    }

    // 创建子节点
    public static void create(ZooKeeper  zkClient) throws Exception {
        // 数据的增删改查
        // 参数1：要创建的节点的路径； 参数2：节点数据 ； 参数3：节点权限 ；参数4：节点的类型
        String nodeCreated = zkClient.create("/eclipse", "hello zk".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    // 获取子节点
    public  static void getChildren(ZooKeeper  zkClient) throws Exception {
        List<String> children = zkClient.getChildren("/", true);

        for (String child : children) {
            System.out.println(child);
        }

        // 延时阻塞
        Thread.sleep(Long.MAX_VALUE);
    }

}


