package com.huaa.Utils;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/9/9 11:57
 */

public class GenerateTemplateUtil {

    private GenerateTemplateUtil() {}

    private static XContentBuilder builder(String alias) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("order", 1)
                .field("template", alias+"_*")
                .startObject("settings")
                    .startObject("index")
                        .field("number_of_shards", "3")
                        .field("number_of_replicas", "1")
                        .field("refresh_interval", "3s")
                    .endObject()
                .endObject()
                .startObject("aliases")
                    .startObject(alias)
                    .endObject()
                .endObject()
                .startObject("mappings")
                    .startObject("_default_")
                        .startObject("_all").field("enabled", false).endObject()
                        .startObject("properties");
        return builder;
    }

    public static String loggingTemplate(String alias) {
        try {
            return builder(alias)
                    .startObject("id").field("type", "keyword").endObject()
                    .startObject("timestamp").field("type", "date").field("format", DateUtil.DATE_FORMAT).endObject()
                    .startObject("content").field("type", "text").field("index", "no").endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .string();
        } catch (IOException e) {
            throw new RuntimeException("build logging template failed", e);
        }
    }

    public static void main(String[] args) {
        System.out.println(loggingTemplate("logging"));
    }

}
