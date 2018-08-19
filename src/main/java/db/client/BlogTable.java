package db.client;

import Utils.JsonUtil;
import data.Blog;
import db.ESUtil;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.rest.RestStatus;

import java.util.Map;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/8/19 17:17
 */

public class BlogTable {

    private static String INDEX_NAME = "website";
    private static String TYPE_NAME = "blog";


    private BlogTable() {
    }

    private static GetRequest createGetRequest() {
        return new GetRequest(INDEX_NAME).type(TYPE_NAME);
    }

    public static Blog get(String id) {
        GetRequest request = createGetRequest().id(id);
        GetResponse response = ESUtil.get(request);
        return response.isExists() ? JsonUtil.fromJson(response.getSourceAsString(), Blog.class) : null;
    }

    private static IndexRequest createIndexRequest() {
        return new IndexRequest(INDEX_NAME, TYPE_NAME);
    }

    public static boolean create(String id, Blog blog) {
        IndexRequest request = createIndexRequest().id(id);
        Map<String, Object> source = JsonUtil.fromJson(JsonUtil.toJson(blog), Map.class);
        request.source(source);
        IndexResponse response = ESUtil.create(request);
        return response.status() == RestStatus.CREATED;
    }

}
