package com.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZkClient {
    public static String connectString = "hadoop.spark.com:2181";
    public static int sessionTimeout = 2000;
    public ZooKeeper zk = null;
    public String parentNode = "/eclipse";


    public static void main(String[] args) throws Exception {

        // 获取zk连接
        ZkClient zkclient = new ZkClient();
        zkclient.getConnect();

        // 获取servers的子节点信息，从中获取服务器信息列表
        zkclient.getServerList();

        // 业务进程启动
        zkclient.business();
    }

    // 创建到zk的客户端连接
    public  void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

            @Override
            public void process(WatchedEvent event) {

                // 再次启动监听
                try {

                    getServerList();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //
    public  void getServerList() throws Exception {

        // 获取服务器子节点信息，并且对父节点进行监听
        List<String> children = zk.getChildren(parentNode, true);
        ArrayList<String> serversList = new ArrayList<>();

        for (String child : children) {
            byte[] data = zk.getData(parentNode + "/" + child, false, null);

            serversList.add(new String(data));
        }

        // 把servers赋值给成员serverList，已提供给各业务线程使用
       // serversList = servers;

        System.out.println(serversList);
    }

    // 业务功能
    public void business() throws Exception {
        System.out.println("client is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }



}
