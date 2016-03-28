package avro_data_source;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Created by zhanglr on 2016/3/23.
 */
public class KafkaProducerTest {
    public static void main( String[] args )
    {
        Properties properties = new Properties();
        properties.put("zk.connect","10.90.3.38:2181,10.90.3.39:2181,10.50.16.22:2181");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list","10.90.3.38:9092,10.90.3.39:9092,10.50.16.22:9092");
        properties.put("request.required.acks","1");
        properties.put("num.partitions","4");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer<String,String> producer = new Producer<String,String>(producerConfig);
        for (int i = 0; i < 1; i++){
            SimpleDateFormat formatter = new SimpleDateFormat   ("yyyy年MM月dd日 HH:mm:ss SSS");
            Date curDate = new Date(System.currentTimeMillis());//获取当前时间
            String str = formatter.format(curDate);
            String msg = "idoall.org" + i+"="+str;
            String key = i+"";
            producer.send(new KeyedMessage<String, String>("test",key, msg));
        }

    }
}
