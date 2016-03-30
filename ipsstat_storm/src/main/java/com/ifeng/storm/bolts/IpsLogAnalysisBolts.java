package com.ifeng.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.ifeng.constant.IpsFieldsCombination;
import com.ifeng.entities.IpsEntity;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhanglr on 2016/3/29.
 */
public class IpsLogAnalysisBolts extends BaseRichBolt {
    private List<List<String>> cols = new ArrayList<List<String>>();
    private Map<String,Field> fieldMap = new HashMap<String, Field>();
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        cols = new IpsFieldsCombination().getCols();
        Field[] fields = IpsEntity.class.getDeclaredFields();
        AccessibleObject.setAccessible(fields, true);
        for (Field field : fields){
            fieldMap.put(field.getName(),field);
        }
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        IpsEntity en = (IpsEntity) tuple.getValue(0);

        try {
            collector.emit(new Values(en,StringUtils.join(cols.get(0),"_")));
            for (List<String> item : cols) {
                if (item.size() < 4) {
                    IpsEntity t = new IpsEntity();
                    for (String inner : item) {
                        if (fieldMap.containsKey(inner)) {
                            Field f = fieldMap.get(inner);
                            f.set(t,f.get(en));
                        }
                    }
                    collector.emit(new Values(t, StringUtils.join(item,"_")));

                }
            }
            collector.ack(tuple);
        }catch (Exception err){
            err.printStackTrace();
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ipslog"));
    }
}
