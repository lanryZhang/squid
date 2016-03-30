package com.ifeng.configuration;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by zhanglr on 2016/3/30.
 */
public class KafkaSpoutConfig extends PropertiesConfig {

    public KafkaSpoutConfig(String path) {
        super(path);
        super.initFile();
    }

    public SpoutConfig getConfig(){
        Properties pro = this.getProperties();

        BrokerHosts hosts =new ZkHosts(pro.getProperty("metadata.broker.list"));

        SpoutConfig config = new SpoutConfig(hosts,
                pro.getProperty("topic"),pro.getProperty("zkRoot"), pro.getProperty("id"));

        config.zkServers = Arrays.asList(pro.getProperty("zookeeper.connect").split(","));
        config.zkPort=Integer.parseInt(pro.getProperty("zookeeper.port"));

        config.ignoreZkOffsets = true;
        config.id = "ipslog";
        config.scheme = new SchemeAsMultiScheme(new StringScheme());

        return config;
    }
}
