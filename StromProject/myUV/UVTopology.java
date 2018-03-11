package storm.myUV;

import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by shuangmm on 2018/1/12
 */
public class UVTopology {
    public static final String SPOUT_ID = storm.myUV.SourceSpout.class.getSimpleName();
    public static final String UVFMT_ID = SplitBolt.class.getSimpleName();

    public static final String UVSUMBOLT_ID = UvSum.class.getSimpleName();

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, new SourceSpout(), 1);

        builder.setBolt(UVFMT_ID, new SplitBolt(), 4).shuffleGrouping(SPOUT_ID);

        builder.setBolt(UVSUMBOLT_ID, new UvSum(), 1).shuffleGrouping(
                UVFMT_ID);

        Map<String, Object> conf = new HashMap<String, Object>();

        if (args != null && args.length > 0) {
            try {
                StormSubmitter.submitTopology(UVTopology.class.getSimpleName(),
                        conf, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(UVTopology.class.getSimpleName(), conf,
                    builder.createTopology());

        }
    }
}
