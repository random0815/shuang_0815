package storm.myUV;

import org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by shuangmm on 2018/1/12
 */
public class SourceSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1L;

    private SpoutOutputCollector collector;
    private Map stormConf;
    /**
     * 当PVSpout初始化时候调用一次
     *
     * @param conf
     *            The Storm configuration for this spout.
     * @param context
     *            可以获取每个任务的TaskID
     * @param collector
     *            The collector is used to emit tuples from this spout.
     */
    @Override
    public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.stormConf = stormConf;
    }

    /**
     * 死循环，一直会调用
     */
    @Override
    public void nextTuple() {

        // 获取数据源
        try {
            String dataDir = "C:\\Users\\shuangmm\\Desktop\\test";
            File file = new File(dataDir);
            //获取文件列表
            Collection<File> listFiles = FileUtils.listFiles(file, new String[]{"log"},true);

            for (File f : listFiles) {
                //处理文件
                List<String> readLines = FileUtils.readLines(f);
                for (String line : readLines) {
                    this.collector.emit(new Values(line));
                }
                // 文件已经处理完成
                try {
                    File srcFile = f.getAbsoluteFile();
                    File destFile = new File(srcFile + ".done." + System.currentTimeMillis());
                    FileUtils.moveFile(srcFile, destFile);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * Declare the output schema for all the streams of this topology.
     *
     * @param declarer
     *            this is used to declare output stream ids, output fields, and
     *            whether or not each output stream is a direct stream
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
}
