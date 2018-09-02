import com.google.common.collect.Lists;
import com.huaa.test.LogClient;
import com.huaa.test.data.Logging;
import org.junit.BeforeClass;
import org.junit.Test;

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
    public void testStoreLogging() {
        int Items = 20000;
        List<Logging> loggingList = Lists.newArrayList();
        for (int i=Items; i<Items+Items+1; i++) {
            Logging logging = new Logging(String.valueOf(i), "content_"+i, System.currentTimeMillis()+random.nextInt(1000000));
            loggingList.add(logging);
        }
        logClient.storeLogging(loggingList.toArray(new Logging[0]));
        logClient.flush();
    }

    @Test
    public void testQueryLoggingById() {
        List<Logging> results = logClient.queryLoggingById("20001");
        printLogging(results);
    }

    @Test
    public void testQueryByTimeRange() {
        List<Logging> results = logClient.queryByTimeRange(1535901490297L, 1535901582360L);
        printLogging(results);
    }

    private static void printLogging(List<Logging> loggingList) {
        for (Logging logging : loggingList) {
            System.out.println(logging);
        }

    }

}
