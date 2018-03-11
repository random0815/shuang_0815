package homework4;

import org.apache.commons.io.FileUtils;
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
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import trident.TridentWordCount;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by shuangmm on 2018/1/15
 */
public class BrowerCount {
    //计算浏览器数量
    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String line = tuple.getString(0);
            String data[] = line.split("\"");
            if (data.length > 5 && data[5].length() > 3) {
                collector.emit(new Values(data[5]));
            }
        }
    }

    public static StormTopology buildTopology(LocalDRPC drpc) {

            TridentTopology topology = new TridentTopology();
            TridentState wordCounts = topology.newStream("spout1",new SourceSpout()).each(new Fields("sentence"),//循环
                    new Split(), new Fields("word")).groupBy(new Fields("word")).persistentAggregate(new MemoryMapState.Factory(),
                    new Count(), new Fields("count"));//.parallelismHint(16);
            wordCounts.newValuesStream().peek(new Consumer() {
                                                  @Override
                                                  public void accept(TridentTuple input) {
                                                      String w = input.getStringByField("word");
                                                      Long c = input.getLongByField("count");
                                                      System.out.println(w + "  is " + c);
                                                  }
                                              });
                    topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split(), new Fields("word")).groupBy(new Fields(
                            "word")).stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count")).each(new Fields("count"),
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
