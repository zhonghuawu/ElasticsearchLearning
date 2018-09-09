package com.huaa.test.core;

import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import com.huaa.Utils.GsonUtil;
import com.huaa.Utils.SortHelper;
import com.huaa.test.clients.LoggingClient;
import com.huaa.test.data.DataLog;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.util.List;
import java.util.Random;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/9/1 10:22
 */

public class ESClient {

    private TransportClient client;
    private IndicesAdminClient indicesAdminClient;

    private static final String DEFAULT_TYPE = "default_type";

    private static final int MAX_PAGE_SIZE = 50000;

    private String scrollId;

    public ESClient(String esIPs, String clusterName) {
        if (esIPs == null || esIPs.isEmpty() || clusterName == null || clusterName.isEmpty()) {
            System.out.println("please init setting for es(ip, clusterName)");
        }
        initTransportClient(esIPs, clusterName);
    }

    public boolean isExistTemplate(String templateName) {
        GetIndexTemplatesRequest request = new GetIndexTemplatesRequest().names(templateName);
        GetIndexTemplatesResponse response = indicesAdminClient.getTemplates(request).actionGet();
        return !response.getIndexTemplates().isEmpty();
    }

    public boolean putTemplate(String templateName, String templateSource) {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.name(templateName).source(templateSource, XContentType.JSON);
        PutIndexTemplateResponse response = indicesAdminClient.putTemplate(request).actionGet();
        return response.isAcknowledged();
    }

    private static Random random = new Random();

    public static String getDay() {
        String month = "2018-09-";
        return month + random.nextInt(31);
    }

    public boolean store(String index, DataLog... logs) {
        if (logs == null || logs.length == 0) {
            return true;
        }
        BulkRequest bulkRequest = new BulkRequest();
        for (DataLog log : logs) {
            bulkRequest.add(new IndexRequest().index(index+"_"+log.indexPostfixName()).type(DEFAULT_TYPE).source(GsonUtil.toJson(log), XContentType.JSON));
        }
        BulkResponse bulkItemResponses = client.bulk(bulkRequest).actionGet();
        return bulkItemResponses.hasFailures();
    }


    public  <T> List<T> queryAll(String index, Class<T> classOfT, QueryBuilder queryBuilder) {
        SearchRequestBuilder requestBuilder = client.prepareSearch(index).setQuery(queryBuilder).addSort(SortHelper.descSortByTimestamp());
        SearchResponse response = requestBuilder.get();
        System.out.println("took: " + response.getTookInMillis());
        return parseHits(response.getHits(), classOfT);
    }

    public SearchResponse query(String index, QueryBuilder queryBuilder, int pageSize, int page) {
        return client.prepareSearch(index)
                .addSort(SortHelper.descSortByTimestamp())
                .setFrom(page)
                .setSize(pageSize)
                .get();
    }

    public <T> List<T> query(String index, Class<T> classOfT, QueryBuilder queryBuilder, int pageSize, int page) {
        SearchResponse response = query(index, queryBuilder, pageSize, page);
        return parseHits(response.getHits(), classOfT);
    }

    public void initScroll(String index, QueryBuilder queryBuilder, int pageSize) {
        SearchResponse response = client.prepareSearch(index)
                .setTypes(DEFAULT_TYPE)
                .setQuery(queryBuilder)
                .setScroll(TimeValue.timeValueMinutes(1))
                .setSize(pageSize)
                .execute()
                .actionGet();
        scrollId = response.getScrollId();
        System.out.println("scrollId: " + scrollId);
    }

    public <T> List<T> queryByScroll(String index, Class<T> classOfT, QueryBuilder queryBuilder, int pageSize, int page) {
        if (page <= 1) {
            page = 1;
        }
        if (scrollId == null) {
            initScroll(index, queryBuilder, pageSize);
        }
        TimeValue timeValue = TimeValue.timeValueMinutes(10);
        SearchResponse response = null;
        for (int i=1; i <= page; i++) {
            response = client.prepareSearchScroll(scrollId)
                    .setScroll(timeValue)
                    .execute()
                    .actionGet();
            scrollId = response.getScrollId();
            System.out.println("scrollId: " + scrollId);
        }
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        client.clearScroll(clearScrollRequest);
        return parseHits(response.getHits(), classOfT);

    }

    private static <T> List<T> parseHits(SearchHits hits, Class<T> classOfT) {
        List<T> results = Lists.newArrayList();
        for (SearchHit hit : hits) {
            results.add(GsonUtil.fromJson(hit.getSourceAsString(), classOfT));
        }
        return results;
    }


    private void initTransportClient(String esIPs, String clusterName) {
        try {
            Settings.Builder builder = Settings.builder();
            builder.put("cluster.name", clusterName);
            Settings settings = builder.build();
            client = new PreBuiltTransportClient(settings);
            String[] ips = esIPs.split(",");
            for (String ip : ips) {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddresses.forString(ip), 9300));
            }
            System.out.println("build client succeed");
            indicesAdminClient = client.admin().indices();
        } catch (Exception e) {
            System.out.println("build es transport client failed " + e.getCause());
        }
    }

    private static LoggingClient loggingClient;

    public synchronized LoggingClient loggingClient() {
        if (loggingClient == null) {
            loggingClient = new LoggingClient(this);
        }
        return loggingClient;
    }

}
