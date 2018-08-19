package db.client;

import Utils.JsonUtil;
import data.Blog;
import db.ESUtil;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

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

}
