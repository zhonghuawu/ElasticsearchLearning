import com.google.common.collect.Lists;
import com.huaa.test.clients.LoggingClient;
import com.huaa.test.core.ESClient;
import com.huaa.test.data.Logging;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/9/2 22:35
 */

public class LoggingClientTest {

    private static ESClient esClient;
    private static LoggingClient client;
    private static Random random = new Random();

    @BeforeClass
    public static void setup() {
        String ips = "192.168.1.3";
        String clusterName = "elasticsearch";
        esClient = new ESClient(ips, clusterName);
        client = esClient.loggingClient();
    }

    @Test
    public void testUpdateTemplate() {
        client.updateTemplate();
    }

    @Test
    public void testStoreLoggingLog() {
        Logging logging = new Logging("logging_three", "content_three");
        client.store(logging);
    }

    @Test
    public void testStoreLogging() {
        int Items = 2000;
        List<Logging> loggingList = Lists.newArrayList();
        for (int i=0; i<Items+Items+1; i++) {
//            Date date = new Date(System.currentTimeMillis()+random.nextInt(1000000));
            Logging logging = new Logging(String.valueOf(i), "content_"+i);
            loggingList.add(logging);
        }
        client.store(loggingList.toArray(new Logging[0]));
    }

    @Test
    public void testQueryLoggingById() {
        List<Logging> results = client.queryLoggingById("10");
        printLogging(results);
    }

    @Test
    public void testQueryByTimeRange() {
        List<Logging> results = client.queryByTimeRange(1535908379023L, 1535908588464L);
        printLogging(results);
    }

    private static void printLogging(List<Logging> loggingList) {
        for (Logging logging : loggingList) {
            System.out.println(logging);
        }

    }

}
