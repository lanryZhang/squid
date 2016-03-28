package avro_data_source;

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.nio.charset.Charset;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        RpcClient client = RpcClientFactory.getDefaultInstance("10.90.3.38", 41414);
        try {
            while(true) {
                Event eb = EventBuilder.withBody("1123", Charset.forName("UTF8"));

                client.append(eb);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            client.close();
        }
    }
}