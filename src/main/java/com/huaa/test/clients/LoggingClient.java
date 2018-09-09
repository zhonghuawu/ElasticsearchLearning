package com.huaa.test.clients;

import com.huaa.Utils.GenerateTemplateUtil;
import com.huaa.Utils.QueryHelper;
import com.huaa.test.core.ESClient;
import com.huaa.test.data.Logging;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;

import java.util.Date;
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

    public List<Logging> queryAll() {
        QueryBuilder queryBuilder = QueryHelper.matchAllQuery();
        return queryAux(queryBuilder);
    }

    public List<Logging> queryPageByScroll(int pageSize, int page) {
        QueryBuilder queryBuilder = QueryHelper.matchAllQuery();
        return queryByScrollAux(queryBuilder, pageSize, page);
    }

    public List<Logging> queryPage(int pageSize, int page) {
        QueryBuilder queryBuilder = QueryHelper.matchAllQuery();
        return queryAux(queryBuilder, pageSize, page);
    }

    public List<Logging> queryLoggingById(String loggingId) {
        QueryBuilder queryBuilder = new TermQueryBuilder("id", loggingId);
        return queryAux(queryBuilder);
    }

    public List<Logging> queryByTimeRange(long from, long to) {
        QueryBuilder queryBuilder = QueryHelper.timeRangeQuery(from, to);
        return queryAux(queryBuilder);
    }

    public List<Logging> queryByTimeRange(Date from, Date to) {
        QueryBuilder queryBuilder = QueryHelper.timeRangeQuery(from, to);
        return queryAux(queryBuilder);
    }

    public List<Logging> queryAux(QueryBuilder queryBuilder) {
        return client.queryAll(index, Logging.class, queryBuilder);
    }

    public List<Logging> queryAux(QueryBuilder queryBuilder, int pageSize, int page) {
        return client.query(index, Logging.class, queryBuilder, pageSize, page);
    }

    public List<Logging> queryByScrollAux(QueryBuilder queryBuilder, int pageSize, int page) {
        return client.queryByScroll(index, Logging.class, queryBuilder, pageSize, page);
    }

}
