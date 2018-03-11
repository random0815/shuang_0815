package StormLast;

import clojure.lang.IFn;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by shuangmm on 2018/1/22
 */
public class FliterBolt extends BaseRichBolt {
    private OutputCollector collector;
    Double max_wind;
    Double min_wind;
    Double max_power;
    Double min_power;
    String current_time;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        //数据过滤
        String key = input.getStringByField("key");
        String date = input.getStringByField("date");
        Double windspeed = Double.parseDouble(input.getStringByField("windSpeed"));
        String fan_no = input.getStringByField("fan_no");
        Double temp = input.getDoubleByField("temp");
        Double power = Double.parseDouble(input.getStringByField("power"));
        String value = input.getStringByField("value");
        boolean flag = true;

        /*if(!date.equals("2016/1/1") || date.equals("2016-01-01"))
            flag = false;*/
        if(windspeed == -902|| windspeed <3 || windspeed >12 )
            flag = false;
        if(power == -902 || power < -0.5  * 1500 || power > 2 * 1500)
            flag = false;
        if(flag)
            collector.emit("Normal",new Values(key,fan_no,temp,value));
        else
            collector.emit("Abnormal",new Values(key,fan_no,temp,value));


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("Normal",new Fields("key","fan_no","temp","value"));
        declarer.declareStream("Abnormal",new Fields("key","fan_no","temp","value"));

    }
}
