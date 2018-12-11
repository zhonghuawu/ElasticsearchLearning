package com.huaa.rest;

import com.huaa.rest.clients.BlogRestClient;
import com.huaa.rest.core.ESRestClient;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/12/8 23:43
 */

public class RestHighLevelClientTest {

    private static String esips = "192.168.1.6:9200";

    private static ESRestClient esRestClient;
    private static BlogRestClient blogRestClient;
    static {
        esRestClient = new ESRestClient(esips);
        blogRestClient = new BlogRestClient(esRestClient);
    }

    public static void main(String[] args) {

    }

}
