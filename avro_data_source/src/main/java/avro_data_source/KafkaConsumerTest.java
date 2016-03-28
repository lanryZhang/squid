package avro_data_source;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhanglr on 2016/3/23.
 */
public class KafkaConsumerTest implements Runnable {

    public static void main(String[] args) {
        KafkaConsumerTest ct = new KafkaConsumerTest("test");
        ct.run();
    }

    final ConsumerConnector connector;
    final String topic;

    public KafkaConsumerTest(String tpName) {
        this.topic = tpName;
        connector = Consumer.createJavaConsumerConnector(createConfig());
    }

    private ConsumerConfig createConfig() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "10.90.3.38:2181,10.90.3.39:2181,10.50.16.22:2181");
        properties.put("group.id", "1");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("zookeeper.session.timeout.ms", "10000");
        return new ConsumerConfig(properties);
    }

    @Override
    public void run() {
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = connector.createMessageStreams(topicMap);
        KafkaStream<byte[], byte[]> ks = streamMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = ks.iterator();
        while (it.hasNext()) {
            System.err.println("get date:" + new String(it.next().message()));
            try {
                //Thread.sleep(100);
            } catch (Exception err) {
                err.printStackTrace();
            }
        }

        System.err.println("end");
    }
}
