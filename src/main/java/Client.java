import Utils.JsonUtil;
import com.google.common.collect.Lists;
import data.Blog;
import db.ESUtil;
import db.client.BlogTable;
import org.apache.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;

import java.util.Map;

/**
 * @author Huaa
 * @date 2018/8/18
 */

public class Client {

    private static Logger log = Logger.getLogger(Client.class);

    private static String index = "website";
    private static String type = "blog";
    private static long id = 6;

    public static void main(String[] args) {
        storeBlog2();
        getBlog2();
    }

    private static void storeBlog() {
        log.info("store blog");
        Blog blog = new Blog("My second blog entry", "Just trying this out");
        blog.setViews(50);
        blog.setTags(Lists.asList("testing", new String[]{"counting"}));
        Map<String, Object> map = JsonUtil.fromJson(JsonUtil.toJson(blog), Map.class);
        IndexResponse indexResponse = ESUtil.create(index, type, String.valueOf(id), map);
        if (indexResponse.status().getStatus() == 200) {
            System.out.println(indexResponse);
        }
    }

    private static void storeBlog2() {
        log.info("store blog by BlogTable");
        Blog blog = new Blog("My third blog netry", "Just trying this out");
        blog.setViews(100);
        blog.setTags(Lists.asList("tests", new String[]{"testing", "counting"}));
        if (BlogTable.create(String.valueOf(id), blog)) {
            log.info("store third blog success");
        } else {
            log.error("store thid blog failed");
        }
    }

    private static void getBlog() {
        log.info("get blog");
        GetResponse getResponse = ESUtil.get(index, type, String.valueOf(id));
        if (getResponse.isExists()) {
            String json = getResponse.getSourceAsString();
            Blog blog = JsonUtil.fromJson(json, Blog.class);
            System.out.println(blog);
        }
    }

    private static void getBlog2() {
        log.info("get blog by other way");
        Blog blog = BlogTable.get(String.valueOf(id));
        if (blog != null) {
            System.out.println(blog);
        }
    }

}
