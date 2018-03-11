package homework5;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapMapper;
import org.apache.storm.hbase.trident.state.HBaseMapState;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.starter.trident.TridentWordCount;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;

/**
 * Created by shuangmm on 2018/1/17
 */
public class Test {
    public static StormTopology buildTopology(){

        ZkHosts zkHosts = new ZkHosts("172.17.11.182:2181,172.17.11.183:2181,172.17.11.184:2181,172.17.11.185:2181");
        String topic="Trident_Kafka_ms";
        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(zkHosts, topic);
        tridentKafkaConfig.scheme=new SchemeAsMultiScheme(new StringScheme());//还有new TestMessageScheme()
        OpaqueTridentKafkaSpout opaqueTridentKafkaSpout=new OpaqueTridentKafkaSpout(tridentKafkaConfig);

        //HBaseState使用HBaseMapState
        HBaseMapState.Options options = new HBaseMapState.Options();

        options.tableName = "Trident_wordCount";
        options.columnFamily = "result";
        options.mapMapper = new SimpleTridentHBaseMapMapper("q1");
        //创建tridentTopology
        TridentTopology tridentTopology = new TridentTopology();
        tridentTopology.newStream("wordcount",opaqueTridentKafkaSpout)
                .each(new Fields("str"),new TridentWordCount.Split(),new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(HBaseMapState.opaque(options),new Count(),new Fields("count"));

        return tridentTopology.build();
    }
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config conf = new Config();
        conf.setMaxSpoutPending(5);
        conf.put("hbase.conf", new HashMap());
        if (args.length == 0) {
            //本地运行
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("KHTridentWordCount", conf, buildTopology());

        }else if (args.length ==1 ){
            //提交拓扑集群运行
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology());
        } else if (args.length == 2) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[1], conf, buildTopology());
        } else {
            System.out.println("Usage: TridentFileTopology <hdfs url> [topology name]");
        }

    }
}
