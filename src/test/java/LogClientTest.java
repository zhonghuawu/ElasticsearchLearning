import com.google.common.collect.Lists;
import com.huaa.test.LogClient;
import com.huaa.test.data.Logging;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
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

public class LogClientTest {

    private static LogClient logClient;
    private static Random random = new Random();

    @BeforeClass
    public static void setup() {
        String ips = "192.168.1.3";
        String clusterName = "elasticsearch";
        logClient = new LogClient(ips, clusterName);
    }

    @Test
    public void testCreateTemplate() {
        logClient.createLoggingTemplate();
    }

    @Test
    public void testStoreLogging() {
        int Items = 200;
        List<Logging> loggingList = Lists.newArrayList();
        for (int i=0; i<Items+Items+1; i++) {
            Date date = new Date(System.currentTimeMillis()+random.nextInt(1000000));
            Logging logging = new Logging(String.valueOf(i), "content_"+i, date);
            loggingList.add(logging);
        }
        logClient.storeLogging(loggingList.toArray(new Logging[0]));
        logClient.flush();
    }

    @Test
    public void testStoreLogging2() throws IOException {
        String format = "yyyy-MM-dd'T'HH:mm:ssZ";
        DateFormat dateFormat = new SimpleDateFormat(format);
        String json = XContentFactory.jsonBuilder()
                .startObject()
                .field("id","200")
                .field("content", "content_200")
                .field("timestamp", dateFormat.format(new Date()))
                .endObject()
                .string();
        logClient.storeAux("logging-20180900", json);
        logClient.flush();
    }

    @Test
    public void testQueryLoggingById() {
        List<Logging> results = logClient.queryLoggingById("10");
        printLogging(results);
    }

    @Test
    public void testQueryByTimeRange() {
        List<Logging> results = logClient.queryByTimeRange(1535908379023L, 1535908588464L);
        printLogging(results);
    }

    private static void printLogging(List<Logging> loggingList) {
        for (Logging logging : loggingList) {
            System.out.println(logging);
        }

    }

}
