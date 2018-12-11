package com.huaa.learning.db;

import com.huaa.Utils.GsonUtil;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Collection;

/**
 * Desc: Elasticsearch util
 *
 * @author Huaa
 * @date 2018/8/19 15:23
 */

public class ESUtil {

    private static Logger logger = Logger.getLogger(ESUtil.class);

    private static TransportClient client;
    private static BulkProcessor bulkProcessor;

    private static String IPS = "192.168.1.7";
    private static int PORT = 9300;
    private static String clusterName = "elasticsearch";

    private ESUtil() {
    }

    static {
        logger.info("ES util init...");
        init();
        initProcessor();
    }

    private static void init() {
        try {
            logger.info("get transport client");
            Settings settings = Settings.builder().put("cluster.name", clusterName).build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(IPS), PORT));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void initProcessor() {
        bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener()
                {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        System.out.println("before bulk, executionId: "+ executionId);
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        System.out.println("after bulk, executionId: "+ executionId);

                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        System.out.println("after bulk, executionId: "+ executionId+", exception: "+ failure);

                    }
                })
                .setBulkActions(1000)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .build();
    }

    public static TransportClient getESClient() {
        return client;
    }

    public static boolean putTemplate(String templateName, String templateSource) {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest().name(templateName).source(templateSource);
        PutIndexTemplateResponse response = client.admin().indices().putTemplate(request).actionGet();
        return response.isAcknowledged();

    }

    public static void storeBulk(String index, String type, Object o) {
        IndexRequest request = new IndexRequest(index, type).source(GsonUtil.toJson(o), XContentType.JSON);
        bulkProcessor.add(request);
    }

    public static SearchResponse query(String index, String type, QueryBuilder queryBuilder, int pageSize, int page) {
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setQuery(queryBuilder)
                .setSize(pageSize)
                .setFrom((page-1)*pageSize)
                .get();
        return response;
    }

    public static GetResponse get(String index, String type, String id) {
        return client.prepareGet(index, type, id).execute().actionGet();
    }

    public static IndexResponse create(String index, String type, String jsonSource) {
        return client.prepareIndex(index, type).setSource(jsonSource, XContentType.JSON).execute().actionGet();
    }

    public static IndexResponse create(String index, String type, String id, String jsonSource) {
        return client.prepareIndex(index, type, id).setSource(jsonSource, XContentType.JSON).execute().actionGet();
    }

    public static UpdateResponse update(String index, String type, String id, String jsonDoc) {
        return client.prepareUpdate(index, type, id).setDoc(jsonDoc, XContentType.JSON).execute().actionGet();
    }

    public static DeleteResponse delete(String index, String type, String id) {
        return client.prepareDelete(index, type, id).execute().actionGet();
    }

    public static SearchResponse searchAll(String index) {
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        return client.prepareSearch(index).setQuery(queryBuilder).get();
    }

    public static SearchResponse search(String index, String name, Object text) {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery(name, text);
        return client.prepareSearch(index).setQuery(queryBuilder).get();
    }

    public static SearchResponse termSearch(String index, String field, Object value) {
        QueryBuilder queryBuilder = QueryBuilders.termsQuery(field, value);
        return client.prepareSearch(index).setQuery(queryBuilder).get();
    }

    public static SearchResponse termsSearch(String index, String field, Collection<Object> values) {
        QueryBuilder queryBuilder = QueryBuilders.termsQuery(index, field, values);
        return client.prepareSearch(index).setQuery(queryBuilder).get();
    }

    public static SearchResponse rangeSearch(String index, String field, Object from, Object to) {
        QueryBuilder queryBuilder = QueryBuilders.rangeQuery(field).from(from, true).to(to, false);
        return client.prepareSearch(index).setQuery(queryBuilder).get();
    }

    public static SearchResponse existedSearch(String index, String field) {
        QueryBuilder queryBuilder = QueryBuilders.existsQuery(field);
        return client.prepareSearch(index).setQuery(queryBuilder).get();
    }

    public static SearchResponse prefixSearch(String index, String field, String prefix) {
        QueryBuilder queryBuilder = QueryBuilders.prefixQuery(field, prefix);
        return client.prepareSearch(index).setQuery(queryBuilder).get();
    }

    public static SearchResponse wildcardSearch(String index, String field, String regexp) {
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery(field, regexp);
        return client.prepareSearch(index).setQuery(queryBuilder).get();
    }

    public static SearchResponse fuzzySearch(String index, String field, Object value) {
        QueryBuilder queryBuilder = QueryBuilders.fuzzyQuery(field, value);
        return client.prepareSearch(index).setQuery(queryBuilder).get();
    }

    public static SearchResponse typeSearch(String index, String type) {
        QueryBuilder queryBuilder = QueryBuilders.typeQuery(type);
        return client.prepareSearch(index).setQuery(queryBuilder).get();
    }

    public static SearchResponse idsSearch(String index, String ids) {
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds(ids);
        return client.prepareSearch(index).setQuery(queryBuilder).get();
    }

}
