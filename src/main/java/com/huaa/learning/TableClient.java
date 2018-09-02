package com.huaa.learning;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huaa.learning.data.Blog;
import com.huaa.learning.db.client.BlogTable;
import org.apache.log4j.Logger;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Map;
import java.util.Random;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/8/19 20:09
 */

public class TableClient {

    private static Logger log = Logger.getLogger(Client.class);

    private static long id = 1000;

    public static void main(String[] args) {
//        storeBlog();
//        storeBlogBatch();
//        getBlog();
//        updateBlog();
//        delete();
//        queryAll();
//        query();
//        queryRangeTime();
        queryUseTerms();
    }

    private static void storeBlog() {
        log.info("store blog by BlogTable");
        Blog blog = new Blog("My third blog entry", "Just trying this out...");
        blog.setViews(100);
        blog.setTags(Lists.asList("testing", new String[]{"testing", "counting", "second"}));
        if (BlogTable.create(String.valueOf(id), blog)) {
            log.info("store third blog success");
        } else {
            log.error("store third blog failed");
        }
    }

    private static void storeBlogBatch() {
        log.info("store a batch of blog by BlogTable");
        Map<String, Blog> blogMap = Maps.newHashMap();
        Random random = new Random();
        for (int id = 0; id < 100; id++) {
            String idStr = String.valueOf(id);
            Blog blog = new Blog("title " + idStr, "text " + idStr);
            blog.setViews(random.nextInt(100));
            blog.setTags(Lists.asList("testing", new String[]{"id_" + idStr}));
            blog.setTimestamp(System.currentTimeMillis());
            blogMap.put(idStr, blog);
        }
        if (BlogTable.createBatch(blogMap)) {
            log.info("create all blog succeed");
        } else {
            log.info("create part of blog failed");
        }
    }

    private static void getBlog() {
        log.info("get blog");
        Blog blog = BlogTable.get(String.valueOf(id));
        if (blog != null) {
            log.info(blog.toString());
        } else {
            log.warn("get blog failed, id: " + String.valueOf(id));
        }
    }

    private static void updateBlog() {
        log.info("update blog");
        Blog blog = new Blog("blog title 1", "blog text 1");
        blog.setViews(10);
        blog.setTags(Lists.newArrayList("1", "by table"));
        if (BlogTable.create(String.valueOf(id), blog)) {
            log.info("store " + blog.getTitle() + " success");
            getBlog();
            Map<String, Object> doc = Maps.newHashMap();
            doc.put("text", "blog text 1 updated by huaa");
            if (BlogTable.update(String.valueOf(id), doc)) {
                log.info("update " + blog.getTitle() + " success");
            } else {
                log.error("update " + blog.getTitle() + " failed");
            }
            getBlog();
        } else {
            log.info("store " + blog.getTitle() + " failed");
        }

    }

    private static void delete() {
        if (BlogTable.delete(String.valueOf(id))) {
            log.info("delete success, id: " + id);
        } else {
            log.warn("delete failed, id: " + id);
        }
    }

    private static void queryAll() {
        Map<String, Blog> results = BlogTable.queryAll();
        print(results);
    }

    private static void query() {
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("text");
        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("views").gte(50);
        Map<String, Blog> results = BlogTable.query(rangeQueryBuilder);
        print(results);
    }

    private static void queryRangeTime() {
        long from = 1534780086050L;
        long to = System.currentTimeMillis();
        Map<String, Blog> results = BlogTable.queryRangeTime(from, to);
        print(results);
    }

    private static void queryUseTerms() {
        String name = "views";
        Long[] values = new Long[] {89L, 5L};

        name = "text";
//        String[] values = new String[] {"text 28"};
        Map<String, Blog> results = BlogTable.queryUseTerms(name, values);
        print(results);
    }

    private static <T> void print(Map<String, T> map) {
        for (Map.Entry<String, T> entry : map.entrySet()) {
            log.info(String.format("%s: %s", entry.getKey(), entry.getValue().toString()));
        }
    }

}
