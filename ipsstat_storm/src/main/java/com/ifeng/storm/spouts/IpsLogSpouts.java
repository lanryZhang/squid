package com.ifeng.storm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import com.ifeng.configuration.KafkaSpoutConfig;
import storm.kafka.KafkaSpout;

/**
 * Created by zhanglr on 2016/3/29.
 */
public class IpsLogSpouts extends KafkaSpout {

    private String topic;
    private SpoutOutputCollector collector;

    public IpsLogSpouts(String topic) {
        super( new KafkaSpoutConfig("kafka.properties").getConfig());
        this.topic = topic;
    }
}
