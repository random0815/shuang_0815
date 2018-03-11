package StormLast;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by shuangmm on 2018/1/22
 */
public class SplitdataBolt extends BaseRichBolt {
    private OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[]  words= sentence.split(",");
        if(words.length > 20){
            String fan_no = words[1];//风机编号
            String date = words[2].split(" ")[0];//日期包括年月日
            String hour = words[2].split(" ")[1];//小时和分钟
            Double temp = Double.parseDouble(words[13]);//发电机温度
            String key = date + "_" + hour + "_" + fan_no;//作为输入到hbase的主键
            String winSpeed = words[4];//风速
            String power = words[21];//功率
            collector.emit(new Values(key,fan_no,date,winSpeed,power,temp,sentence));
            // 确认：tuple成功处理
            collector.ack(input);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key","fan_no","date","windSpeed","power","temp","value"));

    }
}
