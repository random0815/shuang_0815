package homework4;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by shuangmm on 2018/1/16
 */
public class UvTridentCount {
    public static class Split1 extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String line = tuple.getString(0);
            String data2[] = line.split(" ");
            if(data2.length > 6){
                String sid = data2[0];
                String url = data2[6];
                collector.emit(new Values(url,sid));
                //System.out.println("划分"+sid+url);
            }
            }
        }
        public static class URLIP extends BaseFunction{
            Map<String, Set<String>> counts = new HashMap<String, Set<String>>();
            @Override
            public void execute(TridentTuple tuple, TridentCollector collector) {

                String url = tuple.getString(0);
                String sid = tuple.getString(1);
                Set<String> sids= counts.get(url);
                if (sids==null){
                    sids = new HashSet<String>();
                }
                sids.add(sid);
                counts.put(url,sids);
                collector.emit(new Values(url,counts.get(url),counts.get(url).size()));
            }
        }



    public static StormTopology buildTopology(LocalDRPC drpc) {

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1",new SourceSpout()).each(new Fields("sentence"),//循环
                new Split1(),new Fields("url","sid")).each(new Fields("url","sid"),new URLIP(),new Fields("url1","sid1","count1")).peek(new Consumer() {
            @Override
            public void accept(TridentTuple input) {
                String w = input.getStringByField("url1");
                Integer c = input.getIntegerByField("count1");
                System.out.println(w + "  is " + c);
            }
        })
                .groupBy(new Fields("url1")).persistentAggregate(new MemoryMapState.Factory(),new Count(),new Fields("count"));//.parallelismHint(16);
        /*wordCounts.newValuesStream().peek(new Consumer() {
            @Override
            public void accept(TridentTuple input) {
                String w = input.getStringByField("url1");
                Integer c = input.getIntegerByField("");
                System.out.println(w + "  is " + c);
            }
        });*/
        topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split1(), new Fields("url","sid")).groupBy(new Fields(
                "url")).stateQuery(wordCounts, new Fields("url"), new MapGet(), new Fields("count"))
                .each(new Fields("count"),new FilterNull());
//        .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if (args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));

            for (int i = 0; i < 100; i++) {
                System.out.println("DRPC RESULT: " + drpc.execute("sid", "cat the dog jumped"));
                Thread.sleep(1000);
            }


        }
        else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(null));
        }
    }
}
