package avro_data_source;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

/**
 * Created by zhanglr on 2016/3/28.
 */
public class WordSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private FileReader reader;
    private Boolean completed = false;
    private TopologyContext context;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.context = topologyContext;
        try {
            this.reader = new FileReader(map.get("wordsFile").toString());
        } catch (Exception err) {

        }
        this.collector = spoutOutputCollector;
    }

    @Override
    public void close() {

    }
    @Override
    public void nextTuple() {
        if (completed) {
            try {
                Thread.sleep(1000);
            } catch (Exception err) {

            }
            return;
        }
        String str;
        BufferedReader bufferedReader = new BufferedReader(reader);
        try {
            while ((str = bufferedReader.readLine()) != null) {
                this.collector.emit(new Values(str), str);
            }
        } catch (Exception err) {

        } finally {
            completed = true;
        }
    }

    @Override
    public void ack(Object o) {
        System.out.println("Message is ok" + o);
    }

    @Override
    public void fail(Object o) {
        System.out.println("Message is fail" + o);
    }
}