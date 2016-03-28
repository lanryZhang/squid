package avro_data_source;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by zhanglr on 2016/3/25.
 */
public class StormTest {
    public static void main(String[] args) {
        try {
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("word-reader", new WordSpout());
            builder.setBolt("word-normalizer", new WordNormalizeBolt()).shuffleGrouping("word-reader");
            builder.setBolt("word-count", new WordCountBolt(),1).shuffleGrouping("word-normalizer");

            Config conf = new Config();
            conf.put("wordsFile", args[0]);
            conf.setDebug(true);

            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Getting-Started-Toploie", conf, builder.createTopology());
            Thread.sleep(1000);
            localCluster.shutdown();
        }catch (Exception err){
            err.printStackTrace();
        }
    }
}
