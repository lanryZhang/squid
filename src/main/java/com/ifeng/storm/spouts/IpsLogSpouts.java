package com.ifeng.storm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.ifeng.configuration.KafkaConsumerProperties;
import com.ifeng.configuration.PropertiesConfig;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhanglr on 2016/3/29.
 */
public class IpsLogSpouts extends BaseRichSpout {
    private ConsumerConnector connector;
    private String topic;
    private SpoutOutputCollector collector;
    private Boolean completed = false;
    private TopologyContext context;

    public IpsLogSpouts(){}
    public IpsLogSpouts(String topic) {
        this.connector = Consumer.createJavaConsumerConnector(createConfig());
        this.topic = topic;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ipslog"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        context = topologyContext;
        collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        Map<String,Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topic,1);
        Map<String,List<KafkaStream<byte[], byte[]>>> message = connector.createMessageStreams(topicMap);
        KafkaStream<byte[],byte[]> stream = message.get(topic).get(0);
        ConsumerIterator it = stream.iterator();
        while(it.hasNext()){
            collector.emit(new Values(it.next().message()));
            try{
                Thread.sleep(100);
            }catch (Exception err){

            }
        }
    }

    private ConsumerConfig createConfig() {
        PropertiesConfig config = new KafkaConsumerProperties("kafka.properties");

        return new ConsumerConfig(config.getProperties());
    }
}
