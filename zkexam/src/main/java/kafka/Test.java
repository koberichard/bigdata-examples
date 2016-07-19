package com.meituan.kafka.monitor;

import com.meituan.kafka.monitor.zk.ZkStringSerializer;
import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import scala.collection.JavaConversions;

import java.util.List;

/**
 * Created by matt on 16/7/19.
 */
public class Test {
    public static void main(String[] args) {

        ZkClient zkClient = new ZkClient("10.4.232.70:2181,10.4.232.77:2181,10.4.232.78:2181/kafka08", 100000, 100000, ZkStringSerializer.getInstance());
        List<Broker> brokers = JavaConversions.seqAsJavaList(ZkUtils.getAllBrokersInCluster(zkClient));
        if (brokers.size() != 0) {
            StringBuilder brokerStr = new StringBuilder();
            for (int i = 0; i < brokers.size(); i++) {
                Broker broker = brokers.get(i);
                if (i == 0) {
                    brokerStr.append(broker.host());
                } else {
                    brokerStr.append("," + broker.host());
                }
            }
            System.out.println(brokerStr.toString());

        } else {
            System.out.println("The number of brokers is 0!");
        }


//        try {
//            String hostport="10.4.232.70:2181,10.4.232.77:2181,10.4.232.78:2181";
//            ZooKeeper zooKeeper = new ZooKeeper(hostport, 300000, null);    //创建一个ZooKeeper实例,不设置默认watcher
//            String path = "/kafka08/brokers/ids/1";
//            Stat stat = new Stat();
//            byte[] b = zooKeeper.getData(path, false, stat);    //获取节点的信息及存储的数据
//            System.out.println(stat);
//            System.out.println(new String(b));
//            zooKeeper.close();    //关闭实例
//        } catch (Exception e) {
//            e.printStackTrace();
//        }


    }
}
