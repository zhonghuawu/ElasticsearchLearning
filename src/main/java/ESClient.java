import Utils.JsonUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import data.Blog;
import db.ESUtil;
import org.apache.log4j.Logger;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.rest.RestStatus;

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
        updateBlog();
    }

    private static void storeBlog() {
        log.info("store blog");
        Blog blog = new Blog("My second blog entry", "Just trying this out");
        blog.setViews(50);
        blog.setTags(Lists.asList("testing", new String[]{"counting"}));
        IndexResponse indexResponse = ESUtil.create(index, type, String.valueOf(id), JsonUtil.toJson(blog));
        if (indexResponse.status().getStatus() == 200) {
            System.out.println(indexResponse);
        }
    }

    private static void storeBlog3() {
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

    private static void getBlog() {
        log.info("get blog");
        GetResponse getResponse = ESUtil.get(index, type, String.valueOf(id));
        if (getResponse.isExists()) {
            String json = getResponse.getSourceAsString();
            Blog blog = JsonUtil.fromJson(json, Blog.class);
            System.out.println(blog);
        }
    }

    private static void updateBlog() {
        Map<String, Object> doc = Maps.newHashMap();
        doc.put("text", "text, updated by huaa");
        doc.put("title", "title, updated by huaa");
        UpdateResponse response = ESUtil.update(index, type, String.valueOf(id), JsonUtil.toJson(doc));
        log.info("update response: " + response);
    }

    private static void deleteBlog() {
        getBlog();
        DeleteResponse response = ESUtil.delelte(index, type, String.valueOf(id));
        log.info("delete response: " + response);
    }

}
