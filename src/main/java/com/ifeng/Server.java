package com.ifeng;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.ifeng.storm.bolts.IpsLogAnalysisBolts;
import com.ifeng.storm.bolts.IpsLogInitializeBolts;
import com.ifeng.storm.bolts.IpsLogStatBolts;
import com.ifeng.storm.spouts.IpsLogSpouts;

/**
 * Created by zhanglr on 2016/3/29.
 */
public class Server {
    public static void main( String[] args ) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("logreader",new IpsLogSpouts("IPS_LOG"));
        builder.setBolt("loginitialize",new IpsLogInitializeBolts()).shuffleGrouping("logreader");
        builder.setBolt("loganalysis",new IpsLogAnalysisBolts()).shuffleGrouping("loginitialize");
        builder.setBolt("logstat",new IpsLogStatBolts(),3).fieldsGrouping("loganalysis",new Fields("ipslog"));

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        conf.put(Config.TOPOLOGY_DEBUG,true);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("Getting_Start_Stat_IPS_LOG",conf,builder.createTopology());
        try {
            Thread.sleep(1000);
            localCluster.shutdown();
        }catch (Exception e){}
    }
}
