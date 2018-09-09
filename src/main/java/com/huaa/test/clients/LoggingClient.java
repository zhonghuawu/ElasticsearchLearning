package com.huaa.test.clients;

import com.huaa.Utils.GenerateTemplateUtil;
import com.huaa.test.core.ESClient;
import com.huaa.test.data.Logging;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;

import java.util.List;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/9/9 11:44
 */

public class LoggingClient {

    private static String index = "logging";

    private ESClient client;

    public LoggingClient(ESClient client) {
        this.client = client;
        String templateName = index + "_template";
        if (this.client.isExistTemplate(templateName)) {
            System.out.println("template existed: " + templateName);
        } else {
            String template = GenerateTemplateUtil.loggingTemplate(index);
            this.client.putTemplate(templateName, template);
            System.out.println("create template succeed: " + templateName);
        }
        System.out.println("build logging client succeed");
    }

    public void updateTemplate() {
        String templateName = index + "_template";
        String template = GenerateTemplateUtil.loggingTemplate(index);
        System.out.println("template: " + template);
        this.client.putTemplate(templateName, template);
    }
    public void store(Logging... logs) {
        this.client.store(index, logs);
    }

    public List<Logging> queryLoggingById(String loggingId) {
        QueryBuilder queryBuilder = new TermQueryBuilder("id", loggingId);
        return queryLogging(queryBuilder);
    }

    public List<Logging> queryByTimeRange(long from, long to) {
        QueryBuilder queryBuilder = new RangeQueryBuilder("timestamp").from(from).to(to);
        return queryLogging(queryBuilder);
    }

    public List<Logging> queryLogging(QueryBuilder queryBuilder) {
        return client.queryAll(index, Logging.class, queryBuilder);
    }

}
