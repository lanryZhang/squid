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
    public static void main(String[] args) {
        try {
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("logreader", new IpsLogSpouts("IPS_LOG"),5);
            builder.setBolt("loginitialize", new IpsLogInitializeBolts(),2).shuffleGrouping("logreader");
            builder.setBolt("loganalysis", new IpsLogAnalysisBolts(),2).shuffleGrouping("loginitialize");
            builder.setBolt("logstat", new IpsLogStatBolts()).fieldsGrouping("loganalysis",new Fields("ipslog"));


            Config conf = new Config();
            //conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
            conf.put(Config.TOPOLOGY_DEBUG, true);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());

            //remote Submit
//        Config conf = new Config();
//        conf.setNumWorkers(2);
//        conf.setDebug(true);
//
//        Map stormConf = Utils.readStormConfig();
//        stormConf.put("nimbus.host", "10.90.3.38");
//        stormConf.putAll(conf);
//
//        Nimbus.Client client = NimbusClient.getConfiguredClient(stormConf).getClient();
//        String inputJar = "D:\\SourceCode\\JavaIntellij\\IpsLogCollect\\out\\IpsLogCollect.jar";
//
//            NimbusClient nimbus = new NimbusClient(stormConf, "10.90.3.38", 6627);
//
//
//// 使用 StormSubmitter 提交 jar 包
//        String uploadedJarLocation = StormSubmitter.submitJar(stormConf, inputJar);
//        String jsonConf = JSONValue.toJSONString(stormConf);
//        nimbus.getClient().submitTopology("remotetopology", uploadedJarLocation, jsonConf, builder.createTopology());
            Thread.sleep(1000);
            //localCluster.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
