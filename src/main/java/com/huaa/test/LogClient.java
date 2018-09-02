package com.huaa.test;

import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import com.huaa.Utils.JsonUtil;
import com.huaa.test.data.AttrHistory;
import com.huaa.test.data.Logging;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Random;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/9/1 10:22
 */

public class LogClient {

    private TransportClient client;
    private IndicesAdminClient indicesAdminClient;
    private BulkProcessor bulkProcessor;

    private static final String DEFAULT_TYPE = "default_type";
    private static final String DateFormat = "yyyyMMdd";
    private static final java.text.DateFormat DATE_FORMAT = new SimpleDateFormat(DateFormat);

    public LogClient(String esIPs, String clusterName){
        if (esIPs == null || esIPs.isEmpty() || clusterName == null || clusterName.isEmpty()) {
            System.out.println("please init setting for es(ip, clusterName)");
        }

        initTransportClient(esIPs, clusterName);
        initBulkProcessor();
        createTemplate();
    }

    private static String LoggingIndexName = "logging";
    private static String LoggingTempalte = "{\n" +
            "  \"order\": 1,\n" +
            "  \"template\": \"logging-*\",\n" +
            "  \"settings\": {\n" +
            "    \"index\": {\n" +
            "      \"number_of_shards\": \"3\",\n" +
            "      \"number_of_replicas\": \"1\",\n" +
            "      \"refresh_interval\": \"5s\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"mappings\": {\n" +
            "    \"_default_\": {\n" +
            "      \"properties\": {\n" +
            "        \"id\": {\n" +
            "          \"type\": \"keyword\"\n" +
            "        },\n" +
            "        \"timestamp\": {\n" +
            "          \"type\": \"long\"\n" +
            "        },\n" +
            "        \"content\": {\n" +
            "          \"type\": \"string\",\n" +
            "          \"index\": false\n" +
            "        }\n" +
            "      },\n" +
            "      \"_all\": {\n" +
            "        \"enabled\": false\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"aliases\": {\n" +
            "    \"logging\": {}\n" +
            "  }\n" +
            "}";

    private void createTemplate() {
        GetIndexTemplatesRequest request = new GetIndexTemplatesRequest();
        request.names(AttrHistory.indexName() + "_template");
        GetIndexTemplatesResponse response = indicesAdminClient.getTemplates(request).actionGet();
        List<IndexTemplateMetaData> templateMetaDataList = response.getIndexTemplates();
        for (IndexTemplateMetaData templateMetaData : templateMetaDataList) {
            if (templateMetaData.template() == null) {
                String templateName = templateMetaData.name();
                createTemplate(templateName + "_template", "");
            }
        }
    }

    private void createTemplate(String templateName, String templateSource) {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.name(templateName).source(templateSource, XContentType.JSON);
        indicesAdminClient.putTemplate(request);
        indicesAdminClient.preparePutTemplate(templateName).setTemplate(templateSource).get();
    }

    public void storeAttrHistory(AttrHistory... attrHistorys) {

    }

    private static Random random = new Random();
    public static int getDay() {
        int month = 20180900;
        return random.nextInt(31) + month;
    }

    public void storeLogging(Logging... loggings) {
        String index = Logging.indexName();
        storeAux(index, loggings);
    }

    public void flush() {
        bulkProcessor.flush();
    }

    private void storeAux(String index, Object... objects) {
        if (objects == null || objects.length == 0) {
            return;
        }
        for (Object object : objects) {
            String source = JsonUtil.toJson(object);
            bulkProcessor.add(new IndexRequest().index(index+ "-" + String.valueOf(getDay())).type(DEFAULT_TYPE).source(source, XContentType.JSON));
            System.out.println("source: " + source);
        }
    }

    public List<Logging> queryLoggingById(String loggingId) {
        QueryBuilder queryBuilder = new TermQueryBuilder("id", loggingId);
        return queryLogging(queryBuilder);
    }

    public List<Logging> queryByTimeRange(long from, long to) {
        QueryBuilder queryBuilder = new RangeQueryBuilder("timestamp").from(from).to(to);
        return queryLogging(queryBuilder);
    }

    public List<Logging> queryLogging(QueryBuilder queryBuilder) {
        return queryAux(Logging.class, Logging.indexName(), queryBuilder);
    }

    public <T> List<T> queryAux(Class<T> classOfT, String index, QueryBuilder queryBuilder) {
        this.flush();
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

    private void initBulkProcessor() {
        try {
            bulkProcessor = BulkProcessor.builder(
                    client,
                    new BulkProcessor.Listener() {
                        @Override
                        public void beforeBulk(long executionId,
                                               BulkRequest request) {
                        }

                        @Override
                        public void afterBulk(long executionId,
                                              BulkRequest request,
                                              BulkResponse response) {
                        }

                        @Override
                        public void afterBulk(long executionId,
                                              BulkRequest request,
                                              Throwable failure) {
                        }
                    })
                    .setBulkActions(10000)
                    .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(5))
                    .setConcurrentRequests(1)
                    .setBackoffPolicy(
                            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                    .build();
        } catch (Exception e) {
            System.out.println("create bulk processor failed");
        }
    }

}
