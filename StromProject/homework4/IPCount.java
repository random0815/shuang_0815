package homework4;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Created by shuangmm on 2018/1/16
 */
public class IPCount {
    public static class Split2 extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String line = tuple.getString(0);
            String data1[] = line.split(" ");
            if(data1.length>6){
                String sid = data1[0];
                String url = data1[6];
                collector.emit(new Values(url,sid));
                System.out.println("分割"+sid+url);
            }

            }
        }


    public static StormTopology buildTopology(LocalDRPC drpc) {

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1",new SourceSpout()).each(new Fields("sentence"),//循环
                new Split2(), new Fields("url","sid")).groupBy(new Fields("url","sid")).persistentAggregate(new MemoryMapState.Factory(),
                new Count(), new Fields("count"));//.parallelismHint(16);
        wordCounts.newValuesStream().peek(new Consumer() {
            @Override
            public void accept(TridentTuple input) {
                String w = input.getStringByField("url");
                String s = input.getStringByField("sid");
                Long c = input.getLongByField("count");
                System.out.println("用户"+ s + "网址"+ w +"  is " + c);
            }
        });
        topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split2(), new Fields("url","sid")).groupBy(new Fields(
                "url","sid")).stateQuery(wordCounts, new Fields("url"), new MapGet(), new Fields("count")).each(new Fields("count"),
                new FilterNull());
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
                System.out.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
                Thread.sleep(1000);
            }


        }
        else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(null));
        }
    }
}
