package com.huaa.rest.clients;

import com.google.common.collect.Lists;
import com.huaa.Utils.GsonUtil;
import com.huaa.rest.core.ESRestClient;
import com.huaa.rest.data.Blog;
import com.huaa.rest.data.TemplateUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.internal.Join;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.util.List;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/12/9 1:01
 */

public class BlogRestClient {
    private static Logger logger = LogManager.getLogger(BlogRestClient.class);

    private ESRestClient client;

    private static String alias = "blog";

    public BlogRestClient(ESRestClient client) {
        this.client = client;
        try {
            boolean result = putTemplate();
            if (! result)
            {
                logger.error("putTemplate failed, {}", alias);
            }
        } catch (IOException e) {
            logger.error("putTemplate failed, {}", alias, e);
        }
    }

    private boolean putTemplate() throws IOException {
        String templateName = Join.join("-", alias, "template");
        XContentBuilder templateSource = TemplateUtil.blogTemplate(alias);
        return client.putTemplate(templateName, templateSource);
    }

    private static String joinIndexName(String suffix)
    {
        return Join.join("-", alias, suffix);
    }

    public boolean store(String suffix, Blog blog) throws IOException {
        return client.index(joinIndexName(suffix), blog);
    }


    public List<Blog> query(String field, Object value, int pageSize, int page) throws IOException {
        QueryBuilder queryBuilder = QueryBuilders.termQuery(field, value);
        List<String> responses = client.search(alias, queryBuilder, pageSize, page);
        List<Blog> results = Lists.newArrayList();
        responses.forEach(response -> results.add(GsonUtil.fromJson(response, Blog.class)));
        return results;
    }


}
