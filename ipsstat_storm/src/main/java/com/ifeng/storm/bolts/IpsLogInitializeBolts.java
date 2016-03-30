package com.ifeng.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.ifeng.entities.IpsEntity;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by zhanglr on 2016/3/29.
 */
public class IpsLogInitializeBolts extends BaseRichBolt{

    private OutputCollector collector;
    private TopologyContext context;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.context = topologyContext;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String sentence = tuple.getString(0);
            String[] arr = sentence.split(" ");
            String reg = ".* Redirect Service.*";
            String live3GReg = ".* 3GRedirect Service.*";
            String liveReg = ".* LiveAllocation Service.*";
            String hm = "00:00";
            IpsEntity en = new IpsEntity();
            if (arr.length > 25) {
                hm = arr[4].substring(0, arr[4].lastIndexOf(":"));
                en.setHm(hm);
                en.setCreateDate(getNowDate());
                en.setHostIp(arr[1]);
                if (sentence.matches(reg)) {
                    en.setRequestType("0");
                } else if (sentence.matches(liveReg)) {
                    en.setRequestType("1");
                } else if (sentence.matches(live3GReg)) {
                    en.setRequestType("2");
                }
                en.setNodeIp(arr[15]);
                en.setClientType(arr[17]);
                collector.emit(new Values(en));
            }
        }catch (Exception err){
            err.printStackTrace();
            collector.fail(tuple);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ipslog"));
    }

    private String getNowDate() {
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(now);
    }
}
