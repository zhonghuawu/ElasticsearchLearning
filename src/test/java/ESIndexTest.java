import com.huaa.learning.db.ESUtil;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/8/26 23:07
 */

public class ESIndexTest {
    private static TransportClient client;
    private static IndicesAdminClient indicesAdminClient;


    private static String index = "article";
    private static String type = "content";

    @BeforeClass
    public static void beforeClass() {
        client = ESUtil.getESClient();
        indicesAdminClient = client.admin().indices();
    }

    @AfterClass
    public static void afterClass() {
    }

    @Test
    public void createIndexAndMapping() throws IOException {
//        FileOutputStream os = new FileOutputStream(new File("mapping.json"));
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject()
                .startObject("properties")
                .startObject("author").field("type", "string").endObject()
                .startObject("title").field("type", "string").endObject()
                .startObject("content").field("type", "string").endObject()
                .startObject("price").field("type", "string").endObject()
                .startObject("view").field("type", "string").endObject()
                .startObject("tags").field("type", "string").endObject()
                .startObject("date").field("type", "date")
                .field("format", "yyyy-MM-dd HH:mm:ss").endObject()
                .endObject()
                .endObject();
//        System.out.println(mapping.string());
//        os.write(mapping.string().getBytes());
//        os.close();

        CreateIndexResponse response = indicesAdminClient.prepareCreate(index).addMapping(type, mapping).execute().actionGet();
        System.out.println(response);
    }

    public BulkProcessor getBulkProcessor() {

        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("" + executionId + ": " + request.getDescription());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("" + executionId + ": " + request.getDescription() + ", " + response);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("" + executionId + ": " + request.getDescription() + ", " + failure.getCause());
            }
        })
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))
                .setBulkActions(1000)
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
        return bulkProcessor;
    }

    @Test
    public void autoBulkProcessor() throws IOException, InterruptedException {
        BulkProcessor bulkProcessor = getBulkProcessor();
        Random random = new Random();
        for (int i = 1000; i < 100000; i++) {
            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("author", "huaa_" + (random.nextInt(10)))
                    .field("title", "title_" + i)
                    .field("content", "content_" + i)
                    .field("price", "54.00")
                    .field("view", "" + random.nextInt(i + 5))
                    .field("tags", "a,b,c," + i)
                    .field("date", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()))
                    .endObject();
            bulkProcessor.add(new IndexRequest(index, type).id("" + i).source(jsonBuilder));
            System.out.println("store " + i + ": " + jsonBuilder.string());
            Thread.sleep(random.nextInt(5) + 1);
        }
    }

}
