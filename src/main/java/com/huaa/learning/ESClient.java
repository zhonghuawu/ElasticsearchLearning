package com.huaa.learning;

import com.huaa.Utils.JsonUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huaa.learning.data.Blog;
import com.huaa.learning.db.ESUtil;
import org.apache.log4j.Logger;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.Map;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/8/19 20:09
 */

public class ESClient {

    private static Logger log = Logger.getLogger(ESClient.class);

    private static String index = "website";
    private static String type = "blog";
    private static long id = 4;

    public static void main(String[] args) {
//        updateBlog();
//        searchAll();
//        search();
//        deleteBlog();
    }

    public static void storeBlog() {
        log.info("store blog");
        Blog blog = new Blog("My second blog entry", "Just trying this out");
        blog.setViews(50);
        blog.setTags(Lists.asList("testing", new String[]{"counting"}));
        IndexResponse indexResponse = ESUtil.create(index, type, String.valueOf(id), JsonUtil.toJson(blog));
        if (indexResponse.status().getStatus() == 200) {
            System.out.println(indexResponse);
        }
    }

    public static void storeBlog3() {
        log.info("store blog by third way");
        Blog blog = new Blog("My forth blog entry", "Just trying this out");
        blog.setViews(10);
        blog.setTags(Lists.asList("test3", new String[]{"first", "second", "third"}));
        IndexResponse response = ESUtil.create(index, type, JsonUtil.toJson(blog));
        if (response.status() == RestStatus.CREATED) {
            log.info("store forth blog success");
        } else {
            log.error("store forth blog failed");
        }
    }

    public static void getBlog() {
        log.info("get blog");
        GetResponse getResponse = ESUtil.get(index, type, String.valueOf(id));
        if (getResponse.isExists()) {
            String json = getResponse.getSourceAsString();
            Blog blog = JsonUtil.fromJson(json, Blog.class);
            System.out.println(blog);
        }
    }

    public static void updateBlog() {
        Map<String, Object> doc = Maps.newHashMap();
        doc.put("text", "text, updated by huaa");
        doc.put("title", "title, updated by huaa");
        UpdateResponse response = ESUtil.update(index, type, String.valueOf(id), JsonUtil.toJson(doc));
        log.info("update response: " + response);
    }

    public static void deleteBlog() {
        getBlog();
        DeleteResponse response = ESUtil.delete(index, type, String.valueOf(id));
        log.info("delete response: " + response);
    }

    public static void searchAll() {
        SearchResponse response = ESUtil.searchAll(index);
        SearchHits hits = response.getHits();
        Map<String, Blog> results = Maps.newHashMap();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            Blog blog = JsonUtil.fromJson(hit.getSourceAsString(), Blog.class);
            results.put(id, blog);
            System.out.println(String.format("%s: %s", id, blog.toString()));
        }
    }

    public static void search() {
        SearchResponse response = ESUtil.search(index, "views", 50L);
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            Blog blog = JsonUtil.fromJson(hit.getSourceAsString(), Blog.class);
            System.out.println(String.format("%s: %s", id, blog.toString()));
        }
    }

}
