package com.huaa.learning.db;

import com.huaa.Utils.QueryHelper;
import com.huaa.learning.ESClient;
import com.huaa.learning.data.Blog;
import com.huaa.learning.db.client.BlogTable;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/11/18 23:18
 */

public class BlogClient {


    private static String INDEX_NAME = "blog";
    private static String TYPE_NAME = "d_type";
    static {
        putTemplate();
    }

    public static void main(String[] args) {
        storeTest();
        queryTest();
    }

    public static void storeTest() {
        long count = 1000 * 10;
        Random random = new Random();
        for (long i = 0; i<count; i++) {
            Blog blog = new Blog("title_"+i, "text_"+i);
            blog.setTimestamp(System.currentTimeMillis() - random.nextLong() % (1000L * 60 * 60 * 24 * 7));
            storeBulk(blog);
        }
    }


    public static void queryTest() {
        int pageSize = 100;
        int page = 1;
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        SearchResponse response = query(queryBuilder, pageSize, page);
        System.out.println("response.status: " + response.status());
        if (response.status() == RestStatus.OK)
        {
            SearchHits hits = response.getHits();
            for (SearchHit hit : hits)
            {
                System.out.println(hit.getSourceAsString());
            }
        }
        else
        {

        }
    }

    private static DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    public static void storeBulk(Blog blog) {
        ESUtil.storeBulk(INDEX_NAME + "_" + dateFormat.format(blog.getTimestamp()), TYPE_NAME, blog);
    }

    public static SearchResponse query(QueryBuilder queryBuilder, int pageSize, int page) {
        return ESUtil.query(INDEX_NAME, TYPE_NAME, queryBuilder, pageSize, page);
    }

    public static void putTemplate() {
        String templateName = INDEX_NAME + "_template";
        String templateSource = generateBlogTemplate();
        ESUtil.putTemplate(templateName, templateSource);
        System.out.println("put blog template succeed");
    }


    public static String generateBlogTemplate() {
        String alias = INDEX_NAME;
        try {
            return XContentFactory.jsonBuilder()
                    .startObject()
                    .field("order", 1)
                    .field("template", alias + "_*")
                    .startObject("alias")
                        .startObject(alias)
                        .endObject()
                    .endObject()
                    .startObject("settings")
                        .field("index.number_of_shards", 3)
                        .field("index.number_of_replicas", 1)
                        .field("refresh_interval", "3s")
                    .endObject()
                    .startObject("mappings")
                        .startObject("_default_")
                            .startObject("properties")
                                .startObject("title").field("type", "keyword").endObject()
                                .startObject("text").field("type", "keyword").endObject()
                                .startObject("views").field("type", "long").endObject()
                                .startObject("tags").field("type", "keyword").field("index", "false").endObject()
                                .startObject("timestamp").field("type", "date").endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                    .endObject()
                    .string();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
