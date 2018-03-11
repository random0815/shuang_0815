package storm.myUV;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

/**
 * Created by shuangmm on 2018/1/12
 */
public class UvSum extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    //Map<String, Map<String,Integer>> counts = new HashMap<String, List<String>>();
    Map<String, Map<String,Integer>> counts = new HashMap<String,  Map<String,Integer>>();
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(Tuple input) {

        String url = input.getStringByField("url");
        String sid = input.getStringByField("sid");
        //Map<String, Integer> hashMap = new HashMap<>();
        Map<String,Integer> urls= counts.get(sid);
        if(urls == null){
            urls = new HashMap<>();
        }
        Integer value = urls.get(url);
        if (value == null) {
            urls.put(url, 1);
        }else{
            urls.put(url, value + 1);
        }

        //sids.add(url);
        counts.put(sid,urls);
        System.out.println("用户ip    :       网址+访问次数");
        for (Map.Entry<String, Map<String,Integer>> e : counts.entrySet()) {
            System.out.println(e.getKey() + ":" + e.getValue());
        }
       /* for (String key:counts.keySet()) {
            System.out.println(key+":"+ " "+counts.get(key).size());
        }*/
        System.out.println("============");
    }




    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
