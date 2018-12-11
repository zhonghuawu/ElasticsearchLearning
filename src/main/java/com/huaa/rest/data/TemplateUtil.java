package com.huaa.rest.data;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/12/9 0:57
 */

public class TemplateUtil {

    private TemplateUtil() {}

    public static XContentBuilder blogTemplate(String alias) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {

        }
        builder.endObject();
        return builder;
    }

}
