import com.google.common.collect.Lists;
import com.huaa.Utils.DateUtil;
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
        int items = 2000;
        List<Logging> loggingList = Lists.newArrayList();
        for (int i=0; i<items; i++) {
//            Date date = new Date(System.currentTimeMillis()+random.nextInt(1000000));
            Logging logging = new Logging(String.valueOf(random.nextInt(items)), "content_"+random.nextInt(items));
            loggingList.add(logging);
        }
        for (int i=0; i<3; i++) {
            client.store(loggingList.toArray(new Logging[0]));
            System.out.println("wait for 10 seconds... " + i);
            try {
                Thread.sleep(10*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testQueryPage() {
        List<Logging> results = client.queryPage(49999, 3);
        printLogging(results);
    }

    @Test
    public void testQueryPageByScroll() {
        List<Logging> results = client.queryPageByScroll(200, 6);
        printLogging(results);
    }

    @Test
    public void testQueryAll() {
        List<Logging> results = client.queryAll();
        printLogging(results);
    }

    @Test
    public void testQueryLoggingById() {
        List<Logging> results = client.queryLoggingById("10");
        printLogging(results);
    }

    @Test
    public void testQueryByTimeRange() {
        String pastStr = "2018-09-09T16:10:59+0800";
        Date past = DateUtil.parse(pastStr);
        List<Logging> results = client.queryByTimeRange(past, new Date());
        printLogging(results);
    }

    private static void printLogging(List<Logging> loggingList) {
        if (loggingList == null) {
            System.out.println("loggingList is null");
        }
        System.out.println("size: " + loggingList.size());
//        for (Logging logging : loggingList) {
//            System.out.println(logging);
//        }

    }

}
