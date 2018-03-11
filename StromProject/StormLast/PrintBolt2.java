package StormLast;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by shuangmm on 2018/1/22
 */
public class PrintBolt2 extends BaseRichBolt {
    private OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        //String l = input.getStringByField("label");
        String k = input.getStringByField("key");
        String v = input.getStringByField("value");
        System.out.println(k + "-----> 22222222<<<<<<" + v);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
