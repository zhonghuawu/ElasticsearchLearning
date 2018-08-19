package db;

import org.apache.log4j.Logger;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Map;

/**
 * Desc: Elasticsearch util
 *
 * @author Huaa
 * @date 2018/8/19 15:23
 */

public class ESUtil {

    private static Logger logger = Logger.getLogger(ESUtil.class);

    private static TransportClient client;

    private static String IPS = "192.168.1.6";
    private static int PORT = 9300;

    private ESUtil() {
    }

    static {
        logger.info("ES util init...");
        init();
    }

    private static void init() {
        try {
            logger.info("get transport client");
            Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(IPS), PORT));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static TransportClient getESClient() {
        return client;
    }

    public static GetResponse get(String index, String type, String id) {
        return client.prepareGet(index, type, id).execute().actionGet();
    }

    public static GetResponse get(GetRequest request) {
        return client.get(request).actionGet();
    }

    public static IndexResponse create(String index, String type, String id, Map<String, Object> map) {
        return client.prepareIndex(index, type, id)
                .setSource(map).execute().actionGet();
    }

    public static IndexResponse create(IndexRequest request) {
        return client.index(request).actionGet();
    }

}
