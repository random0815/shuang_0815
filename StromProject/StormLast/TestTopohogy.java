package StormLast;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;

import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

//import org.apache.storm.kafka.*;


/**
 * Created by shuangmm on 2018/1/14
 */
public class TestTopohogy {

    static Logger logger = LoggerFactory.getLogger(TestTopohogy.class);
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {


        String topic = "topic_data";
        String zkRoot = "/kafka-storm";
        String id = "old";
        BrokerHosts brokerHosts = new ZkHosts("172.17.11.182:2181,172.17.11.183:2181,172.17.11.184:2181,172.17.11.185:2181");//
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
       // spoutConfig.forceFromStart = true;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        //设置一个spout用来从kaflka消息队列中读取数据并发送给下一级的bolt组件，此处用的spout组件并非自定义的，而是storm中已经开发好的KafkaSpout
        builder.setSpout("KafkaSpout", new KafkaSpout(spoutConfig));
        builder.setBolt("data-spilter", new SplitdataBolt()).shuffleGrouping("KafkaSpout");
        builder.setBolt("Filter", new FliterBolt(), 1).shuffleGrouping("data-spilter");
        builder.setBolt("window",new WindowMonitor().withWindow(new BaseWindowedBolt.Duration(30, TimeUnit.SECONDS),
                           new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS)),1).shuffleGrouping("Filter","Normal");
        builder.setBolt("jdbc", PersistentBolt.getJdbcInsertBolt(), 1).shuffleGrouping("window");
        //builder.setBolt("print1",new PrintBolt()).shuffleGrouping("Filter","Normal");
        //builder.setBolt("print2",new PrintBolt2()).shuffleGrouping("Filter","Abnormal");
        Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setNumAckers(0);
        conf.setDebug(false);
        Config.setMessageTimeoutSecs(conf,40000);


/*        SimpleHBaseMapper mapper1 = new SimpleHBaseMapper();
        SimpleHBaseMapper mapper2 = new SimpleHBaseMapper();
        //设置表名
        HBaseBolt hBaseBolt1 = new HBaseBolt("Normal_ms", mapper1).withConfigKey("hbase.conf");
        HBaseBolt hBaseBolt2 = new HBaseBolt("Abnormal_ms", mapper1).withConfigKey("hbase.conf");
        //result为列族名
        mapper1.withColumnFamily("result");
        mapper1.withRowKeyField("key");
        mapper1.withColumnFields(new Fields("value"));
        mapper2.withColumnFamily("result");
        mapper2.withRowKeyField("key");
        mapper2.withColumnFields(new Fields("value"));
        Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setNumAckers(0);
        conf.setDebug(false);

        Map<String, Object> hbConf = new HashMap<String, Object>();
        hbConf.put("hbase.rootdir", "hdfs://172.17.11.182:9000/hbase");
        hbConf.put("hbase.zookeeper.quorum", "172.17.11.182:2181");
        conf.put("hbase.conf", hbConf);

        // hbase-bolt
        builder.setBolt("hbase1", hBaseBolt1, 1).shuffleGrouping("writer","Normal");
        builder.setBolt("hbase2", hBaseBolt2, 1).shuffleGrouping("writer","Abnormal");*/

        if (args != null && args.length > 0) {
            //提交topology到storm集群中运行
            StormSubmitter.submitTopology("sufei-topo", conf, builder.createTopology());
        } else {
            //LocalCluster用来将topology提交到本地模拟器运行，方便开发调试
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount", conf, builder.createTopology());
        }
    }
}

