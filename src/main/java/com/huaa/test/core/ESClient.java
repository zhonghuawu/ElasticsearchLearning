package com.huaa.test.core;

import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import com.huaa.Utils.JsonUtil;
import com.huaa.test.clients.LoggingClient;
import com.huaa.test.data.DataLog;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
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
            bulkRequest.add(new IndexRequest().index(index+"_"+log.indexPostfixName()).type(DEFAULT_TYPE).source(JsonUtil.toJson(log), XContentType.JSON));
        }
        BulkResponse bulkItemResponses = client.bulk(bulkRequest).actionGet();
        return bulkItemResponses.hasFailures();
    }


    public  <T> List<T> queryAll(String index, Class<T> classOfT, QueryBuilder queryBuilder) {
        SearchRequestBuilder requestBuilder = client.prepareSearch(index).setQuery(queryBuilder).addSort("timestamp", SortOrder.DESC);
        SearchResponse response = requestBuilder.get();
        System.out.println("took: " + response.getTookInMillis());
        SearchHits hits = response.getHits();
        List<T> results = Lists.newArrayList();
        for (SearchHit hit : hits) {
            results.add(JsonUtil.fromJson(hit.getSourceAsString(), classOfT));
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
