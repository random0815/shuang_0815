package storm.myUV;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by shuangmm on 2018/1/12
 */
public class SplitBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;

    OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(Tuple input) {

        try {

            String line = input.getStringByField("line");
            String sid = line.split(" ")[0];
            String url = line.split(" ")[6];
            this.collector.emit(new Values(url,sid));
            this.collector.ack(input);


        } catch (Exception e) {
            e.printStackTrace();
            this.collector.fail(input);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url","sid"));
    }
}
