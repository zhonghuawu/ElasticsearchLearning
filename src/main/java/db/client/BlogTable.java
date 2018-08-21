package db.client;

import Utils.JsonUtil;
import com.google.common.collect.Maps;
import data.Blog;
import db.ESUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Map;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/8/19 17:17
 */

public class BlogTable {

    private static TransportClient client;

    static {
        client = ESUtil.getESClient();
    }

    private BlogTable() {
    }

    public static Blog get(String id) {
        GetRequest request = BlogRequestHelper.buildGetRequest(id);
        GetResponse response = client.get(request).actionGet();
        return response.isExists() ? JsonUtil.fromJson(response.getSourceAsString(), Blog.class) : null;
    }

    public static boolean create(String id, Blog blog) {
        IndexRequest request = BlogRequestHelper.buildIndexRequest(id).source(JsonUtil.toJson(blog), XContentType.JSON);
        IndexResponse response = client.index(request).actionGet();
        return response.status() == RestStatus.CREATED;
    }

    public static boolean createBatch(Map<String, Blog> blogMap) {
        BulkRequest requests = new BulkRequest();
        for (Map.Entry<String, Blog> entry : blogMap.entrySet()) {
            IndexRequest request = BlogRequestHelper.buildIndexRequest();
            request.id(entry.getKey()).source(JsonUtil.toJson(entry.getValue()), XContentType.JSON);
            requests.add(request);
        }
        BulkResponse responses = client.bulk(requests).actionGet();
        return responses.hasFailures();
    }

    public static boolean update(String id, Blog blog) {
        UpdateRequest request = BlogRequestHelper.buildUpdateRequest(id).doc(JsonUtil.toJson(blog), XContentType.JSON);
        UpdateResponse response = client.update(request).actionGet();
        return response.status() == RestStatus.OK;
    }

    public static boolean update(String id, Map<String, Object> doc) {
        UpdateRequest request = BlogRequestHelper.buildUpdateRequest(id).doc(doc);
        UpdateResponse response = client.update(request).actionGet();
        return response.status() == RestStatus.OK;
    }

    public static boolean delete(String id) {
        DeleteRequest request = BlogRequestHelper.buildDeleteRequest(id);
        DeleteResponse response = client.delete(request).actionGet();
        return response.status() == RestStatus.OK;
    }

    public static Map<String, Blog> queryAll() {
        SearchRequest request = BlogRequestHelper.buildSearchRequest();
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        request.source(new SearchSourceBuilder().query(queryBuilder));
        SearchResponse response = client.search(request).actionGet();
        SearchHits hits = response.getHits();
        Map<String, Blog> results = Maps.newHashMap();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            Blog blog = JsonUtil.fromJson(hit.getSourceAsString(), Blog.class);
            results.put(id, blog);
        }
        return results;
    }

    public static Map<String, Blog> query(QueryBuilder queryBuilder) {
        SearchRequest request = BlogRequestHelper.buildSearchRequest();
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.query(queryBuilder);
        request.source(ssb);
        SearchResponse response = client.search(request).actionGet();
        SearchHits hits = response.getHits();
        Map<String, Blog> results = Maps.newHashMap();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            Blog blog = JsonUtil.fromJson(hit.getSourceAsString(), Blog.class);
            results.put(id, blog);
        }
        return results;
    }

    public static Map<String, Blog> queryRangeTime(long from, long to) {
        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("timestamp").from(from).to(to);
        SearchRequest request = BlogRequestHelper.buildSearchRequest();
        request.source(new SearchSourceBuilder().query(rangeQueryBuilder));
        SearchResponse response = client.search(request).actionGet();
        SearchHits hits = response.getHits();
        Map<String, Blog> results = Maps.newHashMap();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            Blog blog = JsonUtil.fromJson(hit.getSourceAsString(), Blog.class);
            results.put(id, blog);
        }
        return results;
    }

    public static Map<String, Blog> queryUseTerms(String name, Object... values) {
        QueryBuilder queryBuilder = QueryBuilders.termsQuery(name, values);
        SearchRequest request = BlogRequestHelper.buildSearchRequest();
        request.source(new SearchSourceBuilder().query(queryBuilder));
        SearchResponse response = client.search(request).actionGet();
        SearchHits hits = response.getHits();
        Map<String, Blog> results = Maps.newHashMap();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            Blog blog = JsonUtil.fromJson(hit.getSourceAsString(), Blog.class);
            results.put(id, blog);
        }
        return results;
    }

}
