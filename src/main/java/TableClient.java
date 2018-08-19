import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import data.Blog;
import db.client.BlogTable;
import org.apache.log4j.Logger;

import java.util.Map;

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
//        getBlog();
//        updateBlog();
        delete();
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

}
