package homework5;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapper;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import org.apache.storm.hbase.trident.state.HBaseState;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.apache.storm.hbase.trident.state.HBaseUpdater;
import org.apache.storm.starter.trident.TridentWordCount;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.config.SlidingDurationWindow;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.tuple.Fields;

import org.apache.storm.tuple.Values;



import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by shuangmm on 2018/1/17
 */
public class WindowTest {


    public static class CountWord extends BaseAggregator<Map<String,Integer>> {

        @Override
        public Map<String, Integer> init(Object o, TridentCollector tridentCollector) {
            return new HashMap<String,Integer>();
        }

        @Override
        public void aggregate(Map<String, Integer> map, TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String word=tridentTuple.getStringByField("word");
            Integer value = map.get(word);
            if (value == null) {
                map.put(word, 1);
            } else {
                map.put(word, value + 1);
            }
        }
        @Override
        public void complete(Map<String, Integer> map, TridentCollector tridentCollector) {

            List<Map.Entry<String,Integer>> list=new ArrayList<Map.Entry<String, Integer>>(map.entrySet());

            Collections.sort(list, new Comparator<Map.Entry<String,Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue()-o1.getValue();
                }
            });

            for(int i=0;i<5&&i<list.size();i++){
                tridentCollector.emit(new Values(String.valueOf(i+1),
                        list.get(i).getKey().toString(),
                        list.get(i).getValue().toString()));
            }

        }
    }
        public static StormTopology buildTopology() {

            FixedBatchSpout fixedBatchSpout = new FixedBatchSpout(new Fields("sentence"), 3,
                    new Values("the cow jumped over the moon"),
                    new Values("the man went to the store and bought some candy"),
                    new Values("four score and seven years ago"),
                    new Values("how many apples can you eat"),
                    new Values("to or not to be the person"));
            fixedBatchSpout.setCycle(true);
            //HBase相关配置

            TridentHBaseMapper tridentHBaseMapper=new SimpleTridentHBaseMapper()
                    .withRowKeyField("rank")
                    .withColumnFamily("result")
                    .withColumnFields(new Fields("word","count"));

            //定义HbaseState的属性类Options
            HBaseState.Options options=new HBaseState.Options()
                    .withConfigKey("hbase.conf")
                    .withMapper(tridentHBaseMapper)
                    .withDurability(Durability.SYNC_WAL)
                    .withTableName("window_ms1");

            //使用工厂类生产HbaseState对象
            StateFactory factory=  new HBaseStateFactory(options);

            //定义时间滑动窗口 每10秒计算过去30秒的数据
            WindowConfig slidingDurationWindow = SlidingDurationWindow.of(new BaseWindowedBolt.Duration(30,
                            TimeUnit.SECONDS),
                    new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS));

            //创建TridentTopology

            TridentTopology topology = new TridentTopology();
            topology.newStream("spout", fixedBatchSpout)
                    .each(new Fields("sentence"), new TridentWordCount.Split(), new Fields("word"))
                    .window(slidingDurationWindow, new Fields("word"), new CountWord(), new Fields("rank", "word", "count"))
                    .partitionPersist(factory, new Fields("rank", "word", "count"), new HBaseUpdater(), new Fields());
            return topology.build();
        }

        public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

            Config conf = new Config();
            conf.setDebug(true);
            conf.put("hbase.conf", new HashMap());
            if (args.length == 0) {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("topN", conf, buildTopology());
            } else {
                conf.setNumWorkers(3);
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
                        buildTopology());
            }
        }
    }


