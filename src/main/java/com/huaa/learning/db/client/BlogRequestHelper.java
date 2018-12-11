package com.huaa.learning.db.client;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;

/**
 * Desc: help to create requests
 *
 * @author Huaa
 * @date 2018/8/19 19:45
 */

public class BlogRequestHelper {

    private static String INDEX_NAME = "blog";
    private static String TYPE_NAME = "_type";

    private BlogRequestHelper()
    {}

    public static GetRequest buildGetRequest(String id) {
        return new GetRequest(INDEX_NAME, TYPE_NAME, id);
    }

    public static IndexRequest buildIndexRequest() {
        return new IndexRequest(INDEX_NAME, TYPE_NAME);
    }

    public static IndexRequest buildIndexRequest(String id) {
        return new IndexRequest(INDEX_NAME, TYPE_NAME, id);
    }

    public static UpdateRequest buildUpdateRequest(String id) {
        return new UpdateRequest(INDEX_NAME, TYPE_NAME, id);
    }

    public static DeleteRequest buildDeleteRequest(String id) {
        return new DeleteRequest(INDEX_NAME, TYPE_NAME, id);
    }

    public static SearchRequest buildSearchRequest() {
        return new SearchRequest(INDEX_NAME).types(TYPE_NAME);
    }

}
