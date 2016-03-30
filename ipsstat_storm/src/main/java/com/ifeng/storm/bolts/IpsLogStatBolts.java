package com.ifeng.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.ifeng.entities.IpsEntity;
import com.ifeng.mongo.MongoFactory;

import java.util.*;

/**
 * Created by zhanglr on 2016/3/29.
 */
public class IpsLogStatBolts extends BaseRichBolt {
    private Map<IpsEntity,Integer> statMap ;
    private String currentTm = "";
    private String currentDate = "";
    private OutputCollector collector;

    @Override
    public void cleanup() {

    }
    private void saveToMongo(Map<IpsEntity,Integer> resMap){
        List<IpsEntity> res = new ArrayList<IpsEntity>();
        for (Map.Entry<IpsEntity, Integer> entry : resMap.entrySet()) {
            IpsEntity en = entry.getKey();
            en.setRequestNum(entry.getValue());
            res.add(en);
        }
        try {
            MongoFactory.getInstance().changeDb("ipstest");
            MongoFactory.getInstance().insert(res, new Date());
        } catch (Exception err) {
        }
    }
    @Override
    public void execute(Tuple tuple) {
        IpsEntity en = (IpsEntity)tuple.getValue(0);
        if ((en.getHm().equals(currentTm) && currentDate.equals(en.getCreateDate()))
                || (currentDate.equals("") && currentTm.equals(""))) {
            if (statMap.containsKey(en)) {
                statMap.put(en, statMap.get(en) + 1);
            } else {
                statMap.put(en, 1);
            }
        }else{
            Map t = statMap;
            currentTm = en.getHm();
            currentDate = en.getCreateDate();
            statMap = new HashMap<IpsEntity, Integer>();
            saveToMongo(t);
        }
        collector.ack(tuple);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        statMap = new HashMap<IpsEntity, Integer>();
        collector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ipslog"));
    }
}
