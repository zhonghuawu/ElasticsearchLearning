package com.huaa.rest.core;

import com.google.common.collect.Lists;
import com.huaa.Utils.GsonUtil;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/12/8 23:49
 */

public class ESRestClient {
    private Logger logger = LogManager.getLogger(ESRestClient.class);

    private RestHighLevelClient client;

    public ESRestClient(String... ips)
    {
        HttpHost[] hosts = new HttpHost[ips.length];
        for (int i=0; i<ips.length; i++)
        {
            String[] address = ips[i].split(":");
            String ip = address[0];
            int port = Integer.valueOf(address[1]);
            hosts[i] = new HttpHost(ip, port);
        }
        RestClientBuilder builder = RestClient.builder(hosts);
        client = new RestHighLevelClient(builder);
        logger.info("build rest-high-level client succeed");
    }

    public RestHighLevelClient getClient()
    {
        return client;
    }

    public void close() throws IOException {
        client.close();
    }

    public boolean putTemplate(String templateName, XContentBuilder templateBuilder) throws IOException {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest()
                .name(templateName)
                .source(templateBuilder);
        AcknowledgedResponse response = client.indices().putTemplate(request, RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

    public boolean index(String indexName, Object object) throws IOException {
        IndexRequest request = new IndexRequest(indexName)
                .source(GsonUtil.toJson(object), XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        return response.status() == RestStatus.CREATED;
    }

    public List<String> search(String indexName, QueryBuilder queryBuilder, int pageSize, int page) throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(queryBuilder)
                .size(pageSize)
                .from((page-1)* pageSize);
        SearchRequest request = new SearchRequest(indexName)
                .source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        List<String> results = Lists.newArrayList();
        for (SearchHit hit : hits)
        {
            results.add(hit.getSourceAsString());
        }
        return results;
    }

}
